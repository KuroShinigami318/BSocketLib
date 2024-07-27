#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"
#include "internal/data/ReadData.h"
#include "internal/data/WriteData.h"
#include "Socket.h"

#if defined(USE_WIN32_API)
#include <WinSock2.h>
#include <ws2tcpip.h>
#include "internal/WinIOContext.h"
#pragma comment(lib, "ws2_32.lib")

static const uint8_t k_clientWorkers = 1;
static const uint16_t k_serverWorkers = 128;

static internal::WinIOContext* BindCompletionPort(utils::dynamic_buffers<uint8_t>& o_dynamicArray, std::shared_mutex& o_mutex, void* i_completionPort, ISocket* i_socket, SocketEvent i_eventType, unsigned int i_numThreads)
{
	std::unique_lock lock(o_mutex);
	if (i_socket == nullptr)
	{
		return nullptr;
	}
	internal::WinIOContext* ioContext = utils::dynamic_array_buffer::find_if<internal::WinIOContext>(o_dynamicArray, [i_socket](const internal::WinIOContext& i_ioContext)
	{
		return i_ioContext.socket == i_socket;
	});
	if (ioContext != nullptr)
	{
		ioContext->ioOperations.emplace(i_eventType);
		return ioContext;
	}
	ioContext = utils::dynamic_array_buffer::push<internal::WinIOContext>(o_dynamicArray, HashObject(i_socket), i_socket, i_eventType, 0, 0);
	if (!ioContext)
	{
		return nullptr;
	}
	const size_t lastIndex = utils::dynamic_array_buffer::size<internal::WinIOContext>(o_dynamicArray) - 1;
	i_completionPort = CreateIoCompletionPort((HANDLE) i_socket->GetNativeSocket(), i_completionPort, (ULONG_PTR)ioContext, i_numThreads);
	if (i_completionPort == nullptr)
	{
		utils::dynamic_array_buffer::erase<internal::WinIOContext>(o_dynamicArray, lastIndex);
		return nullptr;
	}
	return ioContext;
}

using CloseSocketFunc = utils::CallableBound<void(ISocket*)>;

static void HandleCloseSocket(std::shared_mutex& o_mutex, internal::WinIOContext* i_ioContext, std::vector<ISocket*>& o_socketsToBeClosed)
{
	if (std::find(o_socketsToBeClosed.begin(), o_socketsToBeClosed.end(), i_ioContext->socket) == o_socketsToBeClosed.end())
	{
		o_mutex.unlock_shared();
		{
			std::unique_lock lock(o_mutex);
			o_socketsToBeClosed.push_back(i_ioContext->socket);
		}
		o_mutex.lock_shared();
	}
}

SocketReactor::EventData::EventData(std::unique_ptr<IEventHandler> i_eventHandler)
	: eventHandler(std::move(i_eventHandler))
{
}

SocketReactor::SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port)
	: m_initType(i_initType), m_nativeHandle(nullptr)
	, m_workersConfig(utils::threadpool_config(i_initType == InitType::Connect ? k_clientWorkers : k_serverWorkers, "Socket Reactor Workers"))
	, m_workerThreadpool(m_workersConfig)
	, m_socketData(i_socketAF, SOCK_STREAM, IPPROTO_TCP, i_address, i_port)
{
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0)
	{
		HandleError(Status::InitFailed, utils::Format("WSAStartup with error: {}", GetLastError()));
		return;
	}
	if (LOBYTE(wsaData.wVersion) != 2 || HIBYTE(wsaData.wVersion) != 2)
	{
		HandleError(Status::InitFailed, "Could not find a usable version of Winsock.dll");
		return;
	}
	m_nativeHandle = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
	if (m_nativeHandle == nullptr)
	{
		HandleError(Status::InitFailed, "CreateIoCompletionPort");
		return;
	}
	std::unique_ptr<ISocket> socket = std::make_unique<Socket>(this);
	Socket_d nativeSocket = socket->Open(i_socketAF, m_socketData.socketType, m_socketData.socketProtocol);
	if (nativeSocket == INVALID_SOCKET)
	{
		HandleError(Status::InitFailed, utils::Format("OpenSocket with error: {}", GetLastError()));
		return;
	}
	m_sockets.emplace_back(std::move(socket));
	m_status = Status::InitSuccess;
}

SocketReactor::~SocketReactor()
{
	std::shared_lock lock(m_mutex);
	while (m_status == Status::Shuttingdown)
	{
		lock.unlock();
		utils::this_thread::yield();
		utils::this_thread::sleep_for(utils::milisecs(1));
		lock.lock();
	}
	if (m_status != Status::Shutdowned)
	{
		lock.unlock();
		Shutdown();
		lock.lock();
	}
}

void SocketReactor::Shutdown()
{
	std::unique_lock lock(m_mutex);
	if (m_status == Status::Shuttingdown)
	{
		CRASH("Trying to call shutdown again!!!");
	}
	m_status = Status::Shuttingdown;
	utils::Epilogue cleanup([this]() { m_status = Status::Shutdowned; });
	for (utils::async_waitable<void>& waitable : m_waitables)
	{
		PostQueuedCompletionStatus(m_nativeHandle, 0, 0, NULL);
	}
	utils::dynamic_array_buffer::deallocate<internal::WinIOContext>(m_nativeBuffer);
	m_sockets.clear();
	WSACleanup();
	CloseHandle(m_nativeHandle);
	lock.unlock();
	m_workerThreadpool.shutdown();
	lock.lock();
	auto eventIt = m_eventsMap.find(SocketEvent::ShuttingDown);
	if (eventIt == m_eventsMap.end())
	{
		return;
	}
	lock.unlock();
	utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, nullptr);
	lock.lock();
}

void SocketReactor::CloseClient(ISocket* i_socket)
{
	{
		std::shared_lock lock(m_mutex);
		auto eventIt = m_eventsMap.find(SocketEvent::CloseConnection);
		if (eventIt != m_eventsMap.end())
		{
			i_socket->SetBlockProcess(true);
			lock.unlock();
			utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(i_socket));
			lock.lock();
		}
	}
	std::unique_lock lock(m_mutex);
	utils::dynamic_array_buffer::erase_if<internal::WinIOContext>(m_nativeBuffer, [i_socket](const internal::WinIOContext& ioContext)
	{
		return ioContext.socket == i_socket;
	});
	std::erase_if(m_sockets, [i_socket](std::unique_ptr<ISocket>& socket)
	{
		return socket.get() == i_socket;
	});
}

void SocketReactor::HandleError(Status i_status, std::string i_locationFailed)
{
	std::unique_lock lock(m_mutex);
	m_status = i_status;
	m_optError = make_error<Error>(ErrorCode::InternalError, "failed in {}", i_locationFailed);
}

void SocketReactor::Update(float)
{
	std::shared_lock sharedLock(m_mutex);
	if (m_status == Status::Shuttingdown)
	{
		return;
	}
	if (m_status == Status::InitFailed)
	{
		CRASH_PLAIN_MSG("{}", m_optError.value());
	}
	while (m_waitables.size() < m_workersConfig.num_threads)
	{
		m_waitables.push_back(utils::async(m_workerThreadpool, [this]()
		{
			DWORD bytesTransferred = 0;
			BOOL isSuccess = FALSE;
			LPOVERLAPPED overlapped = nullptr;
			internal::WinIOContext* ioContext = nullptr;
			internal::WinIOOperation* ioOperation = nullptr;
			isSuccess = GetQueuedCompletionStatus(m_nativeHandle, &bytesTransferred, (PULONG_PTR)&ioContext, &overlapped, INFINITE);

			std::shared_lock sharedLock(m_mutex);
			if (m_status == Status::Shuttingdown || ioContext == nullptr || overlapped == nullptr)
			{
				return;
			}
			ioOperation = reinterpret_cast<internal::WinIOOperation*>(overlapped);
			if (!isSuccess || (isSuccess && bytesTransferred == 0))
			{
				sharedLock.unlock();
				{
					std::unique_lock lock(m_mutex);
					ioContext->ioOperations.erase_if([ioOperation](const internal::WinIOOperation& i_ioOperation)
					{
						return &i_ioOperation == ioOperation;
					});
				}
				sharedLock.lock();
				HandleCloseSocket(m_mutex, ioContext, m_socketsToBeClosed);
				return;
			}

			auto eventIt = m_eventsMap.find(ioOperation->eventType);
			if (eventIt == m_eventsMap.end())
			{
				sharedLock.unlock();
				{
					std::unique_lock lock(m_mutex);
					ioContext->ioOperations.erase_if([ioOperation](const internal::WinIOOperation& i_ioOperation)
					{
						return &i_ioOperation == ioOperation;
					});
				}
				sharedLock.lock();
				return;
			}
			switch (ioOperation->eventType)
			{
			case SocketEvent::ReadStream:
			{
				const char* const readBuffer = ioOperation->wsaBuffers.buf;
				ioOperation->totalBytes = bytesTransferred;
				std::vector<char> bytes(readBuffer, readBuffer + ioOperation->totalBytes);
				internal::data::ReadData readData(bytes, *ioContext->socket);
				sharedLock.unlock();
				size_t byteToRead = 0;
				const bool shouldCloseConnection = !utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(&readData)).value();
				ProcessAsyncRawData(reinterpret_cast<void*>(&byteToRead), SocketEvent::ReadStream, ioContext->socket).ignoreResult();
				{
					std::unique_lock lock(m_mutex);
					ioContext->ioOperations.erase_if([ioOperation](const internal::WinIOOperation& i_ioOperation)
					{
						return &i_ioOperation == ioOperation;
					});
				}
				sharedLock.lock();
				if (shouldCloseConnection)
				{
					HandleCloseSocket(m_mutex, ioContext, m_socketsToBeClosed);
				}
			}
			break;
			case SocketEvent::WriteStream:
			{
				ioOperation->sentBytes += bytesTransferred;
				if (ioOperation->sentBytes < ioOperation->totalBytes)
				{
					utils::async(m_messageQueue, [this, ioContext, ioOperation]()
					{
						DWORD byteSent = 0;
						ioOperation->wsaBuffers.buf = ioOperation->buffers + ioOperation->sentBytes;
						ioOperation->wsaBuffers.len = ioOperation->totalBytes - ioOperation->sentBytes;
						int result = WSASend(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &byteSent, ioOperation->dwFlags, &ioOperation->overlapped, NULL);
						if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
						{
							{
								std::unique_lock lock(m_mutex);
								ioContext->ioOperations.erase_if([ioOperation](const internal::WinIOOperation& i_ioOperation)
								{
									return &i_ioOperation == ioOperation;
								});
							}
							std::shared_lock sharedLock(m_mutex);
							HandleCloseSocket(m_mutex, ioContext, m_socketsToBeClosed);
						}
					});
				}
				else if (ioOperation->totalBytes > 0)
				{
					size_t sentBytes = (size_t)ioOperation->sentBytes;
					sharedLock.unlock();
					const bool shouldCloseConnection = !utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(&sentBytes)).value();
					{
						std::unique_lock lock(m_mutex);
						ioContext->ioOperations.erase_if([ioOperation](const internal::WinIOOperation& i_ioOperation)
						{
							return &i_ioOperation == ioOperation;
						});
					}
					sharedLock.lock();
					if (shouldCloseConnection)
					{
						HandleCloseSocket(m_mutex, ioContext, m_socketsToBeClosed);
					}
				}
			}
			break;
			default: break;
			}
		}));
	}
	const bool shouldCheckCloseClient = !m_socketsToBeClosed.empty();
	sharedLock.unlock();
	if (shouldCheckCloseClient)
	{
		std::unique_lock lock(m_mutex);
		ISocket* socket = m_socketsToBeClosed.back();
		internal::WinIOContext* ioContext = utils::dynamic_array_buffer::find_if<internal::WinIOContext>(m_nativeBuffer, [socket](const internal::WinIOContext& i_ioContext)
		{
			return socket == i_ioContext.socket && i_ioContext.ioOperations.empty();
		});
		if (ioContext != nullptr)
		{
			m_socketsToBeClosed.pop_back();
			lock.unlock();
			CloseClient(socket);
			lock.lock();
		}
	}
	sharedLock.lock();
	std::erase_if(m_waitables, [](const utils::async_waitable<void>& i_waitable)
	{
		return i_waitable.HasFinished();
	});
	m_messageQueue.dispatch();
}

ISocketReactor::ReactorResult SocketReactor::Run()
{
	if (m_optError.has_value())
	{
		return m_optError.value();
	}

	struct addrinfo hints = { 0 };
	struct addrinfo *addrlocal = NULL, *rp = NULL;
	hints.ai_family = m_socketData.socketAF;
	hints.ai_socktype = m_socketData.socketType;
	hints.ai_protocol = m_socketData.socketProtocol;

	utils::Epilogue clean([&addrlocal]()
	{
		if (addrlocal)
			freeaddrinfo(addrlocal);
	});

	switch (m_initType)
	{
	case InitType::Bind:
	{
		hints.ai_flags = AI_PASSIVE;
		if (getaddrinfo(m_socketData.address.c_str(), std::to_string(m_socketData.port).c_str(), &hints, &addrlocal) != 0 || addrlocal == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "getaddrinfo failed {}", WSAGetLastError());
		}
		for (rp = addrlocal; rp != NULL; rp = rp->ai_next)
		{
			int result = bind(m_sockets.front()->GetNativeSocket(), addrlocal->ai_addr, (int)addrlocal->ai_addrlen);
			if (result != SOCKET_ERROR)
			{
				break;
			}
			m_sockets.pop_back();
			std::unique_ptr<ISocket> socket = std::make_unique<Socket>(this);
			Socket_d nativeSocket = socket->Open(m_socketData.socketAF, m_socketData.socketType, m_socketData.socketProtocol);
			if (nativeSocket == INVALID_SOCKET)
			{
				continue;
			}
			m_sockets.emplace_back(std::move(socket));
		}
		if (rp == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "bind failed {}", WSAGetLastError());
		}
		int result = listen(m_sockets.front()->GetNativeSocket(), SOMAXCONN);
		if (result == SOCKET_ERROR)
		{
			return make_error<Error>(ErrorCode::InternalError, "listen failed {}", WSAGetLastError());
		}

		{
			std::unique_lock lock(m_mutex);
			m_status = Status::Running;
		}
		Socket_d acceptSocket = INVALID_SOCKET;
		std::shared_lock sharedLock(m_mutex);
		while (m_status == Status::Running)
		{
			sharedLock.unlock();
			acceptSocket = WSAAccept(m_sockets.front()->GetNativeSocket(), NULL, NULL, NULL, 0);
			if (acceptSocket == INVALID_SOCKET)
			{
				sharedLock.lock();
				continue;
			}
			ISocket* socketIt = nullptr;
			{
				std::unique_lock lock(m_mutex);
				socketIt = m_sockets.emplace_back(std::make_unique<Socket>(this, acceptSocket)).get();
			}
			sharedLock.lock();
			auto eventIt = m_eventsMap.find(SocketEvent::AcceptConnection);
			if (eventIt != m_eventsMap.end())
			{
				sharedLock.unlock();
				if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(socketIt)).value())
				{
					CloseClient(socketIt);
					sharedLock.lock();
					continue;
				}
				sharedLock.lock();
			}
			sharedLock.unlock();
			size_t byteToRead = 0;
			ProcessAsyncRawData(reinterpret_cast<void*>(&byteToRead), SocketEvent::ReadStream, socketIt).ignoreResult();
			sharedLock.lock();
		}
	}
	break;
	case InitType::Connect:
	{
		if (getaddrinfo(m_socketData.address.c_str(), std::to_string(m_socketData.port).c_str(), &hints, &addrlocal) != 0 || addrlocal == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "getaddrinfo failed {}", WSAGetLastError());
		}
		for (rp = addrlocal; rp != NULL; rp = rp->ai_next)
		{
			int result = connect(m_sockets.front()->GetNativeSocket(), addrlocal->ai_addr, (int)addrlocal->ai_addrlen);
			if (result != SOCKET_ERROR)
			{
				break;
			}
			m_sockets.pop_back();
			std::unique_ptr<ISocket> socket = std::make_unique<Socket>(this);
			Socket_d nativeSocket = socket->Open(m_socketData.socketAF, m_socketData.socketType, m_socketData.socketProtocol);
			if (nativeSocket == INVALID_SOCKET)
			{
				continue;
			}
			m_sockets.emplace_back(std::move(socket));
		}
		if (rp == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "connect failed {}", WSAGetLastError());
		}
		std::shared_lock sharedLock(m_mutex);
		auto eventIt = m_eventsMap.find(SocketEvent::AcceptConnection);
		if (eventIt == m_eventsMap.end())
		{
			sharedLock.unlock();
			goto START_RECV;
		}
		sharedLock.unlock();
		if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(m_sockets.front().get())).value())
		{
			HandleError(Status::Shuttingdown, "Reject accepting connection by client!");
			sharedLock.lock();
			goto SHUTDOWN;
		}
		START_RECV:
		size_t byteToRead = 0;
		ProcessAsyncRawData(reinterpret_cast<void*>(&byteToRead), SocketEvent::ReadStream, m_sockets.front().get()).ignoreResult();
		sharedLock.lock();
	}
	break;
	default: CRASH_PLAIN_MSG("Not handle init type: {}", m_initType);
	}

	SHUTDOWN:
	if (m_optError.has_value())
	{
		return m_optError.value();
	}
	return Ok();
}

ISocketReactor::ReactorResult SocketReactor::ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket)
{
	if (i_socket == nullptr)
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "invalid socket in ProcessAsyncRawData");
	}
	std::shared_lock sharedLock(m_mutex);
	if (std::find(m_socketsToBeClosed.cbegin(), m_socketsToBeClosed.cend(), i_socket) != m_socketsToBeClosed.cend())
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "socket going to be closed!");
	}
	switch (i_eventType)
	{
	case SocketEvent::ReadStream:
	{
		sharedLock.unlock();
		size_t& sizeToRead = *reinterpret_cast<size_t*>(i_rawData);
		if (sizeToRead > DATA_BUFSIZE)
		{
			size_t splitedSize = sizeToRead - DATA_BUFSIZE;
			auto splitReadResult = ProcessAsyncRawData(reinterpret_cast<void*>(&splitedSize), SocketEvent::ReadStream, i_socket);
			if (splitReadResult.isErr())
			{
				return splitReadResult;
			}
			sizeToRead = DATA_BUFSIZE;
		}
		internal::WinIOContext* ioContext = BindCompletionPort(m_nativeBuffer, m_mutex, m_nativeHandle, i_socket, i_eventType, m_workersConfig.num_threads);
		if (ioContext == nullptr)
		{
			std::string errorString = utils::Format("BindCompletionPort with error: {}", WSAGetLastError());
			HandleError(Status::Shuttingdown, errorString);
			sharedLock.lock();
			return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, errorString.c_str());
		}
		sharedLock.lock();
		internal::WinIOOperation* ioOperation = ioContext->ioOperations.back();
		utils::async(m_messageQueue, [this, ioContext, ioOperation]()
		{
			DWORD dwRecvNumBytes = 0;
			int result = WSARecv(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &dwRecvNumBytes, &ioOperation->dwFlags, (LPWSAOVERLAPPED)ioOperation, nullptr);
			if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
			{
				PostQueuedCompletionStatus(m_nativeHandle, 0, (ULONG_PTR)ioContext, (LPWSAOVERLAPPED)ioOperation);
			}
		});
	}
	break;
	case SocketEvent::WriteStream:
	{
		sharedLock.unlock();
		internal::data::WriteData& writeData = *reinterpret_cast<internal::data::WriteData*>(i_rawData);
		if (writeData.size > DATA_BUFSIZE)
		{
			auto splitIt = writeData.end - DATA_BUFSIZE;
			internal::data::WriteData splitWriteData{writeData.bytes, writeData.begin, splitIt, writeData.size - DATA_BUFSIZE };
			auto splitWriteResult = ProcessAsyncRawData(reinterpret_cast<void*>(&splitWriteData), SocketEvent::WriteStream, i_socket);
			if (splitWriteResult.isErr())
			{
				return splitWriteResult;
			}
			writeData.begin = splitIt;
			writeData.size = DATA_BUFSIZE;
		}
		internal::WinIOContext* ioContext = BindCompletionPort(m_nativeBuffer, m_mutex, m_nativeHandle, i_socket, i_eventType, m_workersConfig.num_threads);
		if (ioContext == nullptr)
		{
			std::string errorString = utils::Format("BindCompletionPort with error: {}", WSAGetLastError());
			HandleError(Status::Shuttingdown, errorString);
			sharedLock.lock();
			return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, errorString.c_str());
		}
		sharedLock.lock();
		internal::WinIOOperation* ioOperation = ioContext->ioOperations.back();
		memcpy(ioOperation->buffers, &*writeData.begin, writeData.size);
		ioOperation->wsaBuffers.buf = ioOperation->buffers;
		ioOperation->wsaBuffers.len = writeData.size;
		ioOperation->totalBytes = writeData.size;
		utils::async(m_messageQueue, [this, ioContext, ioOperation]()
		{
			DWORD byteSent = 0;
			int result = WSASend(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &byteSent, ioOperation->dwFlags, (LPWSAOVERLAPPED)ioOperation, nullptr);
			if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
			{
				PostQueuedCompletionStatus(m_nativeHandle, 0, (ULONG_PTR)ioContext, (LPWSAOVERLAPPED)ioOperation);
			}
		});
	}
	break;
	case SocketEvent::CloseConnection:
	{
		internal::WinIOContext* ioContext = utils::dynamic_array_buffer::find_if<internal::WinIOContext>(m_nativeBuffer, [i_socket](const internal::WinIOContext& i_ioContext)
		{
			return i_ioContext.socket == i_socket;
		});
		if (ioContext == nullptr)
		{
			return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "IOContext not found with socket: {}", i_socket->GetNativeSocket());
		}
		HandleCloseSocket(m_mutex, ioContext, m_socketsToBeClosed);
	}
	break;
	default: return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "Unhandled processing event");
	}

	return Ok();
}

ISocketReactor::ReactorResult SocketReactor::RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler)
{
	std::unique_lock lock(m_mutex);
	if (m_eventsMap.find(i_event) != m_eventsMap.end())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event has already been registered!", i_event);
	}
	if (i_eventHandler == nullptr || i_event != i_eventHandler->GetEventType())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event mismatch with the given handler!", i_event);
	}
	auto eventMap = m_eventsMap.try_emplace(i_event, std::move(i_eventHandler));
	EventData& eventData = eventMap.first->second;
	IEventHandler* eventHandler = eventData.eventHandler.get();
	eventHandler->m_connection = eventMap.first->second.cb_handleAction.Connect(eventHandler);

	return Ok();
}

ISocketReactor::ReactorResult SocketReactor::DeregisterEventHandler(SocketEvent i_event)
{
	std::unique_lock lock(m_mutex);
	if (m_eventsMap.find(i_event) == m_eventsMap.end())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event is not registered!", i_event);
	}
	m_eventsMap.erase(i_event);
	return Ok();
}

#endif