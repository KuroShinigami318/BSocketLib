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
constexpr const uint8_t headerSize = sizeof(size_t);

using BindIOCPResult = std::pair<public_buffer_iterator, internal::WinIOOperation*>;

static BindIOCPResult BindCompletionPort(SocketNativeBufferT& o_dynamicArray, SocketMapT& o_socketsMaps, void* i_completionPort, ISocket* i_socket, SocketEvent i_eventType, unsigned int i_numThreads)
{
	if (i_socket == nullptr)
	{
		return BindIOCPResult(o_dynamicArray.end(), nullptr);
	}
	auto foundIt = o_socketsMaps.find(i_socket->GetNativeSocket());
	if (foundIt != o_socketsMaps.end())
	{
		public_buffer_iterator ioContext = foundIt->second;
		auto ioOperation = ioContext->ioOperations.emplace(i_eventType);
		ioOperation->selfIterator = ioOperation;
		return BindIOCPResult(ioContext, &*ioOperation);
	}
	public_buffer_iterator ioContext = utils::dynamic_array_buffer::push<internal::WinIOContext>(o_dynamicArray, HashObject(i_socket), i_socket, i_eventType, 0, 0);
	if (ioContext == o_dynamicArray.end())
	{
		return BindIOCPResult(o_dynamicArray.end(), nullptr);
	}
	ioContext->selfIterator = ioContext;
	const size_t lastIndex = o_dynamicArray.size() - 1;
	i_completionPort = CreateIoCompletionPort((HANDLE) i_socket->GetNativeSocket(), i_completionPort, (ULONG_PTR)&*ioContext, i_numThreads);
	if (i_completionPort == nullptr)
	{
		utils::dynamic_array_buffer::erase<internal::WinIOContext>(o_dynamicArray, lastIndex);
		return BindIOCPResult(o_dynamicArray.end(), nullptr);
	}
	o_socketsMaps.emplace(i_socket->GetNativeSocket(), ioContext);
	return BindIOCPResult(ioContext, &ioContext->ioOperations.back());
}

static void HandleCloseSocket(std::shared_mutex& o_mutex, public_buffer_iterator i_publicIterator, SocketMapT& o_socketsToBeClosed)
{
	if (o_socketsToBeClosed.find(i_publicIterator->socket->GetNativeSocket()) == o_socketsToBeClosed.end())
	{
		o_mutex.unlock_shared();
		{
			std::unique_lock lock(o_mutex);
			o_socketsToBeClosed.try_emplace(i_publicIterator->socket->GetNativeSocket(), i_publicIterator->selfIterator);
		}
		o_mutex.lock_shared();
	}
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
	m_hostSocket = std::make_unique<Socket>(this);
	Socket_d nativeSocket = m_hostSocket->Open(i_socketAF, m_socketData.socketType, m_socketData.socketProtocol);
	if (nativeSocket == INVALID_SOCKET)
	{
		HandleError(Status::InitFailed, utils::Format("OpenSocket with error: {}", GetLastError()));
		return;
	}
	m_status = Status::InitSuccess;
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
	auto eventIt = m_eventsMap.find(SocketEvent::ShuttingDown);
	if (eventIt != m_eventsMap.end())
	{
		lock.unlock();
		utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, nullptr);
		lock.lock();
	}
	for (utils::async_waitable<void>& waitable : m_waitables)
	{
		lock.unlock();
		waitable.Wait();
		lock.lock();
	}
	utils::dynamic_array_buffer::deallocate<internal::WinIOContext>(m_nativeBuffer);
	m_sockets.clear();
	WSACleanup();
	CloseHandle(m_nativeHandle);
}

void SocketReactor::CloseClient(ISocket* i_socket)
{
	{
		std::shared_lock lock(m_mutex);
		auto eventIt = m_eventsMap.find(SocketEvent::CloseConnection);
		if (eventIt != m_eventsMap.end())
		{
			lock.unlock();
			utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(i_socket));
			lock.lock();
		}
	}
	std::unique_lock lock(m_mutex);
	m_sockets.erase(i_socket->GetNativeSocket());
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
			isSuccess = GetQueuedCompletionStatus(m_nativeHandle, &bytesTransferred, (PULONG_PTR)&ioContext, &overlapped, 0);

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
					ioContext->ioOperations.erase(ioOperation->selfIterator);
				}
				sharedLock.lock();
				ioContext->socket->SetBlockProcess(true);
				HandleCloseSocket(m_mutex, ioContext->selfIterator, m_socketsToBeClosed);
				return;
			}

			auto eventIt = m_eventsMap.find(ioOperation->eventType);
			if (eventIt == m_eventsMap.end())
			{
				sharedLock.unlock();
				{
					std::unique_lock lock(m_mutex);
					ioContext->ioOperations.erase(ioOperation->selfIterator);
				}
				sharedLock.lock();
				return;
			}
			std::unique_lock opLock(ioOperation->mutex);
			switch (ioOperation->eventType)
			{
			case SocketEvent::ReadStream:
			{
				bool shouldCloseConnection = false;
				const char* const readBuffer = ioOperation->buffers;
				size_t headerBytes = 0;
				DWORD totalBytes = 0;
				internal::data::ReadData readData(ioOperation->receivedBuffers, *ioContext->socket);
				if (ioOperation->totalBytes == 0)
				{
					std::copy(readBuffer, readBuffer + headerSize, reinterpret_cast<char*>(&ioOperation->totalBytes));
					bytesTransferred -= headerSize;
					ioOperation->receivedBuffers.clear();
					ioOperation->receivedBuffers.reserve(ioOperation->totalBytes);
					headerBytes = headerSize;
				}
				ioOperation->sentBytes += bytesTransferred;
				if (ioOperation->sentBytes < ioOperation->totalBytes)
				{
					ioOperation->receivedBuffers.insert(ioOperation->receivedBuffers.end(), readBuffer + headerBytes, readBuffer + headerBytes + bytesTransferred);
					DWORD dwRecvNumBytes = 0;
					memset(ioOperation->buffers, 0, DATA_BUFSIZE);
					ioOperation->wsaBuffers.buf = ioOperation->buffers;
					ioOperation->wsaBuffers.len = DATA_BUFSIZE;
					int result = WSARecv(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &dwRecvNumBytes, &ioOperation->dwFlags, (LPWSAOVERLAPPED)ioOperation, nullptr);
					if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
					{
						shouldCloseConnection = true;
						goto CLEAN_OPERATION;
					}
					goto CLOSE_CONNECTION;
				}
				totalBytes = headerBytes + bytesTransferred - (ioOperation->sentBytes - ioOperation->totalBytes);
				ioOperation->receivedBuffers.insert(ioOperation->receivedBuffers.end(), readBuffer + headerBytes, readBuffer + totalBytes);
				sharedLock.unlock();
				shouldCloseConnection = !utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(&readData)).value();
				if (ioOperation->sentBytes > ioOperation->totalBytes && !shouldCloseConnection)
				{
					memmove(ioOperation->buffers, ioOperation->buffers + totalBytes, DATA_BUFSIZE - totalBytes);
					memset(ioOperation->buffers + DATA_BUFSIZE - totalBytes, 0, totalBytes);
					totalBytes = ioOperation->sentBytes - ioOperation->totalBytes;
					ioOperation->totalBytes = 0;
					ioOperation->sentBytes = 0;
					if (totalBytes > headerSize)
					{
						PostQueuedCompletionStatus(m_nativeHandle, totalBytes, (ULONG_PTR) & *ioContext, (LPWSAOVERLAPPED)ioOperation);
						goto CLOSE_CONNECTION;
					}
				}
				CLEAN_OPERATION:
				opLock.unlock();
				{
					std::unique_lock lock(m_mutex);
					ioContext->ioOperations.erase(ioOperation->selfIterator);
				}
				if (!shouldCloseConnection)
				{
					ProcessAsyncRawData(nullptr, SocketEvent::ReadStream, ioContext->socket).ignoreResult();
				}
				sharedLock.lock();
				CLOSE_CONNECTION:
				if (shouldCloseConnection)
				{
					ioContext->socket->SetBlockProcess(true);
					HandleCloseSocket(m_mutex, ioContext->selfIterator, m_socketsToBeClosed);
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
						std::unique_lock opLock(ioOperation->mutex);
						DWORD byteSent = 0;
						ioOperation->wsaBuffers.buf = ioOperation->buffers + ioOperation->sentBytes;
						ioOperation->wsaBuffers.len = ioOperation->totalBytes - ioOperation->sentBytes;
						int result = WSASend(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &byteSent, ioOperation->dwFlags, &ioOperation->overlapped, NULL);
						if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
						{
							{
								opLock.unlock();
								std::unique_lock lock(m_mutex);
								ioContext->ioOperations.erase(ioOperation->selfIterator);
							}
							std::shared_lock sharedLock(m_mutex);
							ioContext->socket->SetBlockProcess(true);
							HandleCloseSocket(m_mutex, ioContext->selfIterator, m_socketsToBeClosed);
						}
					});
				}
				else if (ioOperation->totalBytes > 0)
				{
					size_t sentBytes = (size_t)ioOperation->sentBytes;
					sharedLock.unlock();
					const bool shouldCloseConnection = !utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(&sentBytes)).value();
					{
						opLock.unlock();
						std::unique_lock lock(m_mutex);
						ioContext->ioOperations.erase(ioOperation->selfIterator);
					}
					sharedLock.lock();
					if (shouldCloseConnection)
					{
						ioContext->socket->SetBlockProcess(true);
						HandleCloseSocket(m_mutex, ioContext->selfIterator, m_socketsToBeClosed);
					}
				}
			}
			break;
			default: break;
			}
		}));
	}
	const bool shouldCheckCloseClient = !m_socketsToBeClosed.empty();
	if (shouldCheckCloseClient)
	{
		public_buffer_iterator ioContext = m_socketsToBeClosed.begin()->second;
		if (ioContext->ioOperations.empty())
		{
			ISocket* obsoleteSocket = ioContext->socket;
			sharedLock.unlock();
			{
				std::unique_lock lock(m_mutex);
				m_socketsMappedNativeContexts.erase(ioContext->socket->GetNativeSocket());
				m_socketsToBeClosed.erase(ioContext->socket->GetNativeSocket());
				utils::dynamic_array_buffer::erase<internal::WinIOContext>(m_nativeBuffer, ioContext->selfIterator);
			}
			CloseClient(obsoleteSocket);
		}
	}
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
		std::shared_lock sharedLock(m_mutex);
		if (m_status != Status::InitSuccess)
		{
			return make_error<Error>(ErrorCode::InternalError, "SocketReactor not in InitSuccess state");
		}
		for (rp = addrlocal; rp != NULL; rp = rp->ai_next)
		{
			int result = bind(m_hostSocket->GetNativeSocket(), rp->ai_addr, (int)rp->ai_addrlen);
			if (result != SOCKET_ERROR)
			{
				break;
			}
			m_hostSocket.reset(new Socket(this));
			Socket_d nativeSocket = m_hostSocket->Open(m_socketData.socketAF, m_socketData.socketType, m_socketData.socketProtocol);
			if (nativeSocket == INVALID_SOCKET)
			{
				rp = NULL;
				break;
			}
		}
		if (rp == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "bind failed {}", WSAGetLastError());
		}
		int result = listen(m_hostSocket->GetNativeSocket(), SOMAXCONN);
		if (result == SOCKET_ERROR)
		{
			return make_error<Error>(ErrorCode::InternalError, "listen failed {}", WSAGetLastError());
		}

		sharedLock.unlock();
		{
			std::unique_lock lock(m_mutex);
			m_status = Status::Running;
		}
		sharedLock.lock();
		Socket_d acceptSocket = INVALID_SOCKET;
		while (m_status == Status::Running)
		{
			sharedLock.unlock();
			acceptSocket = WSAAccept(m_hostSocket->GetNativeSocket(), NULL, NULL, NULL, 0);
			if (acceptSocket == INVALID_SOCKET)
			{
				sharedLock.lock();
				continue;
			}
			SocketsT::iterator socketIt = m_sockets.end();
			{
				std::unique_lock lock(m_mutex);
				socketIt = m_sockets.emplace(acceptSocket, utils::make_unique<Socket>(this, acceptSocket)).first;
			}
			sharedLock.lock();
			auto eventIt = m_eventsMap.find(SocketEvent::AcceptConnection);
			if (eventIt != m_eventsMap.end())
			{
				sharedLock.unlock();
				if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(&*socketIt->second)).value())
				{
					CloseClient(&*socketIt->second);
					sharedLock.lock();
					continue;
				}
				sharedLock.lock();
			}
			sharedLock.unlock();
			size_t byteToRead = 0;
			ProcessAsyncRawData(reinterpret_cast<void*>(&byteToRead), SocketEvent::ReadStream, &*socketIt->second).ignoreResult();
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
		std::shared_lock sharedLock(m_mutex);
		if (m_status != Status::InitSuccess)
		{
			return make_error<Error>(ErrorCode::InternalError, "SocketReactor not in InitSuccess state");
		}
		for (rp = addrlocal; rp != NULL; rp = rp->ai_next)
		{
			int result = connect(m_hostSocket->GetNativeSocket(), rp->ai_addr, (int)rp->ai_addrlen);
			if (result != SOCKET_ERROR)
			{
				break;
			}
			m_hostSocket.reset(new Socket(this));
			Socket_d nativeSocket = m_hostSocket->Open(m_socketData.socketAF, m_socketData.socketType, m_socketData.socketProtocol);
			if (nativeSocket == INVALID_SOCKET)
			{
				rp = NULL;
				break;
			}
		}
		if (rp == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "connect failed {}", WSAGetLastError());
		}
		m_sockets.emplace(m_hostSocket->GetNativeSocket(), std::move(m_hostSocket));
		auto eventIt = m_eventsMap.find(SocketEvent::AcceptConnection);
		if (eventIt == m_eventsMap.end())
		{
			sharedLock.unlock();
			goto START_RECV;
		}
		sharedLock.unlock();
		if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, reinterpret_cast<void*>(&*m_sockets.begin()->second)).value())
		{
			HandleError(Status::InitFailed, "Reject accepting connection by client!");
			sharedLock.lock();
			goto SHUTDOWN;
		}
		START_RECV:
		{
			std::unique_lock lock(m_mutex);
			m_status = Status::Running;
		}
		size_t byteToRead = 0;
		ProcessAsyncRawData(reinterpret_cast<void*>(&byteToRead), SocketEvent::ReadStream, &*m_sockets.begin()->second).ignoreResult();
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
	return utils::Ok();
}

ISocketReactor::ReactorResult SocketReactor::ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket)
{
	if (i_socket == nullptr)
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "invalid socket in ProcessAsyncRawData");
	}
	if (i_socket->IsBlockProcess())
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "socket going to be closed!");
	}
	std::shared_lock sharedLock(m_mutex);
	switch (i_eventType)
	{
	case SocketEvent::ReadStream:
	{
		sharedLock.unlock();
		std::unique_lock lock(m_mutex);
		BindIOCPResult bindResult = BindCompletionPort(m_nativeBuffer, m_socketsMappedNativeContexts, m_nativeHandle, i_socket, i_eventType, m_workersConfig.num_threads);
		public_buffer_iterator ioContext = bindResult.first;
		internal::WinIOOperation* ioOperation = bindResult.second;
		if (ioContext == m_nativeBuffer.end())
		{
			std::string errorString = utils::Format("BindCompletionPort with error: {}", WSAGetLastError());
			HandleError(Status::InitFailed, errorString);
			return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, errorString.c_str());
		}
		lock.unlock();
		sharedLock.lock();
		if (ioContext->ioOperations.find_if([ioOperation](const internal::WinIOOperation& i_ioOperation) { return i_ioOperation.eventType == SocketEvent::ReadStream && ioOperation != &i_ioOperation; }) != ioContext->ioOperations.cend())
		{
			return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::AlreadyInProgress, "ReadAsync already in progress!");
		}
		DWORD dwRecvNumBytes = 0;
		int result = WSARecv(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &dwRecvNumBytes, &ioOperation->dwFlags, (LPWSAOVERLAPPED)ioOperation, nullptr);
		if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
		{
			PostQueuedCompletionStatus(m_nativeHandle, 0, (ULONG_PTR)&*ioContext, (LPWSAOVERLAPPED)ioOperation);
		}
	}
	break;
	case SocketEvent::WriteStream:
	{
		sharedLock.unlock();
		internal::data::WriteData& writeData = *reinterpret_cast<internal::data::WriteData*>(i_rawData);
		size_t dataLimitSize = DATA_BUFSIZE - headerSize;
		size_t totalBytes = 0;
		size_t remainingBytes = writeData.size;
		size_t headerBytes = headerSize;
		for (; writeData.begin != writeData.end; writeData.begin += dataLimitSize)
		{
			std::unique_lock lock(m_mutex);
			BindIOCPResult bindResult = BindCompletionPort(m_nativeBuffer, m_socketsMappedNativeContexts, m_nativeHandle, i_socket, i_eventType, m_workersConfig.num_threads);
			public_buffer_iterator ioContext = bindResult.first;
			internal::WinIOOperation* ioOperation = bindResult.second;
			if (ioContext == m_nativeBuffer.end())
			{
				std::string errorString = utils::Format("BindCompletionPort with error: {}", WSAGetLastError());
				HandleError(Status::InitFailed, errorString);
				return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, errorString.c_str());
			}
			std::unique_lock opLock(ioOperation->mutex);
			if (totalBytes == 0)
			{
				std::copy(reinterpret_cast<const char*>(&writeData.size), reinterpret_cast<const char*>(&writeData.size) + headerSize, ioOperation->buffers);
			}
			else
			{
				dataLimitSize = DATA_BUFSIZE;
				headerBytes = 0;
			}
			dataLimitSize = (std::min)(dataLimitSize, remainingBytes);
			memcpy(ioOperation->buffers + headerBytes, &*writeData.begin, dataLimitSize);
			ioOperation->wsaBuffers.buf = ioOperation->buffers;
			ioOperation->wsaBuffers.len = headerBytes + dataLimitSize;
			ioOperation->totalBytes = dataLimitSize;
			totalBytes += dataLimitSize;
			remainingBytes -= dataLimitSize;
			utils::async(m_messageQueue, [this, ioOperation](public_buffer_iterator ioContext)
			{
				DWORD byteSent = 0;
				int result = WSASend(ioContext->socket->GetNativeSocket(), &ioOperation->wsaBuffers, 1, &byteSent, ioOperation->dwFlags, (LPWSAOVERLAPPED)ioOperation, nullptr);
				if (result == SOCKET_ERROR && (ERROR_IO_PENDING != WSAGetLastError()))
				{
					PostQueuedCompletionStatus(m_nativeHandle, 0, (ULONG_PTR)&*ioContext, (LPWSAOVERLAPPED)ioOperation);
				}
			}, ioContext);
		}
	}
	break;
	case SocketEvent::CloseConnection:
	{
		public_buffer_iterator ioContext = utils::dynamic_array_buffer::find_if<internal::WinIOContext>(m_nativeBuffer, [i_socket](const internal::WinIOContext& i_ioContext)
		{
			return i_ioContext.socket == i_socket;
		});
		if (ioContext == m_nativeBuffer.end())
		{
			return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "IOContext not found with socket: {}", i_socket->GetNativeSocket());
		}
		ioContext->socket->SetBlockProcess(true);
		HandleCloseSocket(m_mutex, ioContext, m_socketsToBeClosed);
	}
	break;
	default: return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "Unhandled processing event");
	}

	return utils::Ok();
}

#endif