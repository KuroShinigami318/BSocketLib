#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"
#include "internal/data/ReadData.h"
#include "internal/data/WriteData.h"
#include "Socket.h"

#if defined(USE_POSIX_API)
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/epoll.h>

static const int k_MaxEvents = 64;
static const int k_waitInMs = 333;
static const uint8_t k_clientWorkers = 2;

struct RawDataCache
{
	bool operator==(ISocket* i_socket) const
	{
		return i_socket == socket;
	}
	bool operator==(Socket_d i_socket) const
	{
		return socket ? socket->GetNativeSocket() == i_socket : false;
	}
	RawDataCache(ISocket* i_socket, internal::data::WriteData::bytes_t i_rawData, internal::data::WriteData i_writeData)
		: socket(i_socket), rawData(i_rawData), writeData(i_writeData)
	{
	}
	ISocket* socket;
	internal::data::WriteData::bytes_t rawData;
	internal::data::WriteData writeData;
	SocketNativeBufferT::iterator selfIterator = SocketNativeBufferT::s_end();
};

struct HandleImplement : SocketReactor, IEventHandler, ISocket
{
static void HandleAccept(SocketReactor* thisReactor, Socket_d server_fd)
{
	Socket_d acceptSocket = -1;
	struct epoll_event ev;
	ev.events = EPOLLIN | EPOLLET;

	while (1)
	{
		acceptSocket = accept4(server_fd, NULL, NULL, SOCK_NONBLOCK);
		if (acceptSocket == BS_INVALID_SOCKET)
		{
			if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
			{
				break;
			}
			return;
		}
		ev.data.fd = acceptSocket;
		Socket_d* epoll_socket = (Socket_d*)(thisReactor->*&HandleImplement::m_nativeHandle);
		if (epoll_ctl(*epoll_socket, EPOLL_CTL_ADD, acceptSocket, &ev) == -1)
		{
			continue;
		}
		ISocket* socketIt = nullptr;
		{
			std::unique_lock lock(thisReactor->*&HandleImplement::m_mutex);
			socketIt = &*(thisReactor->*&HandleImplement::m_sockets).emplace<Socket>(thisReactor, acceptSocket);
			using bytes_t = internal::data::WriteData::bytes_t;
			internal::data::WriteData writeData;
			utils::public_dynamic_buffer_iterator<RawDataCache, SocketNativeBufferT::buffer_t> dataCache
			= utils::dynamic_array_buffer::push<RawDataCache>(thisReactor->*&HandleImplement::m_nativeBuffer, socketIt, bytes_t(), writeData);
			dataCache->selfIterator = dataCache;
			dataCache->writeData.bytes = &dataCache->rawData;
			dataCache->writeData.begin = dataCache->writeData.end = dataCache->rawData.end();
			dataCache->writeData.size = 0;
		}
		std::shared_lock sharedLock(thisReactor->*&HandleImplement::m_mutex);
		auto& eventsMap = thisReactor->*&HandleImplement::m_eventsMap;
		auto eventIt = eventsMap.find(SocketEvent::AcceptConnection);
		if (eventIt == eventsMap.end())
		{
			continue;
		}
		sharedLock.unlock();
		if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&HandleImplement::HandleEvent, reinterpret_cast<void*>(socketIt)).value())
		{
			HandleImplement::HandleCloseSocket(thisReactor, socketIt->GetNativeSocket());
			continue;
		}
	}
}

static void HandleReadStream(SocketReactor* thisReactor, Socket_d clientSocket)
{
	if (thisReactor == nullptr)
	{
		return;
	}
	std::vector<char> bytes;
	char buffer[DATA_BUFSIZE];
	while (1)
	{
		ssize_t nBytes = recv(clientSocket, buffer, DATA_BUFSIZE, MSG_DONTWAIT | MSG_NOSIGNAL);
		if (nBytes == -1 && (errno == EWOULDBLOCK || errno == EAGAIN))
		{
			break;
		}
		if (nBytes <= 0)
		{
			HandleCloseSocket(thisReactor, clientSocket);
			break;
		}
		const size_t currentSize = bytes.size();
		bytes.resize(currentSize + nBytes);
		memcpy(bytes.data() + currentSize, buffer, nBytes);
	}
	if (bytes.empty())
	{
		return;
	}
	std::shared_lock lock(thisReactor->*&HandleImplement::m_mutex);
	std::unordered_map<SocketEvent, EventData>& eventsMap = thisReactor->*&HandleImplement::m_eventsMap;
	if (auto eventIt = eventsMap.find(SocketEvent::ReadStream); eventIt != eventsMap.end())
	{
		SocketsT& sockets = thisReactor->*&HandleImplement::m_sockets;
		auto clientIt = sockets.find_if([clientSocket](ISocket& socket)
		{
			return socket.GetNativeSocket() == clientSocket;
		});
		if (clientIt == sockets.end())
		{
			return;
		}
		lock.unlock();
		internal::data::ReadData readData{bytes, *clientIt};
		if (utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&HandleImplement::HandleEvent, reinterpret_cast<void*>(&readData)).value())
		{
			return;
		}
		HandleCloseSocket(thisReactor, clientIt->GetNativeSocket());
	}
}

static void HandleWriteStream(SocketReactor* thisReactor, Socket_d clientSocket)
{
#define OnError() \
	dataCache->writeData.size = 0; \
	dataCache->writeData.begin = dataCache->writeData.end; \
	HandleCloseClient(thisReactor, *dataCache, lock);

	if (thisReactor == nullptr)
	{
		return;
	}
	std::shared_lock lock(thisReactor->*&HandleImplement::m_mutex);
	utils::public_dynamic_buffer_iterator<RawDataCache, SocketNativeBufferT::buffer_t> dataCache = utils::dynamic_array_buffer::find<RawDataCache>(thisReactor->*&HandleImplement::m_nativeBuffer, clientSocket);
	if (dataCache == SocketNativeBufferT::s_end() || dataCache->rawData.empty())
	{
		return;
	}
	size_t totalSent = 0;
	size_t nBytesSending = 0;
	while (1)
	{
		nBytesSending = dataCache->writeData.size > DATA_BUFSIZE ? DATA_BUFSIZE : dataCache->writeData.size;
		ssize_t nBytes = send(clientSocket, &*dataCache->writeData.begin, nBytesSending, MSG_DONTWAIT | MSG_NOSIGNAL);
		if (nBytes == 0 || dataCache->writeData.size == 0)
		{
			break;
		}
		else if (nBytes == -1 && (errno == EWOULDBLOCK || errno == EAGAIN))
		{
			utils::this_thread::yield();
			utils::this_thread::sleep_for(utils::milisecs(k_waitInMs));
			continue;
		}
		else if (nBytes == -1)
		{
			OnError();
			return;
		}
		totalSent += nBytes;
		dataCache->writeData.size -= nBytes;
		dataCache->writeData.begin = dataCache->writeData.begin + nBytes;
	}
	if (totalSent == 0)
	{
		return;
	}
	struct epoll_event ev;
	ev.data.fd = clientSocket;
	ev.events = EPOLLIN | EPOLLET;
	if (epoll_ctl(*(Socket_d*)(thisReactor->*&HandleImplement::m_nativeHandle), EPOLL_CTL_MOD, clientSocket, &ev) == -1)
	{
		OnError();
		return;
	}
	std::unordered_map<SocketEvent, EventData>& eventsMap = thisReactor->*&HandleImplement::m_eventsMap;
	if (auto eventIt = eventsMap.find(SocketEvent::WriteStream); eventIt != eventsMap.end())
	{
		lock.unlock();
		if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&HandleImplement::HandleEvent, reinterpret_cast<void*>(&totalSent)).value())
		{
			dataCache->writeData.size = 0;
			dataCache->writeData.begin = dataCache->writeData.end;
			HandleCloseSocket(thisReactor, clientSocket);
		}
	}
}

static void HandleCloseClient(SocketReactor* thisReactor, RawDataCache& rawDataCache, std::shared_lock<std::shared_mutex>& o_sharedLock)
{
	SocketMapT& socketsToBeClosed = thisReactor->*&HandleImplement::m_socketsToBeClosed;
	if (socketsToBeClosed.find(rawDataCache.socket->GetNativeSocket()) != socketsToBeClosed.cend())
	{
		return;
	}
	o_sharedLock.unlock();
	{
		std::unique_lock lock(thisReactor->*&HandleImplement::m_mutex);
		socketsToBeClosed.try_emplace(rawDataCache.socket->GetNativeSocket(), rawDataCache.selfIterator);
	}
	o_sharedLock.lock();
}

static void HandleCloseSocket(SocketReactor* thisReactor, Socket_d clientSocket)
{
	std::shared_lock sharedLock(thisReactor->*&HandleImplement::m_mutex);
	SocketMapT& socketsToBeClosed = thisReactor->*&HandleImplement::m_socketsToBeClosed;
	if (socketsToBeClosed.find(clientSocket) != socketsToBeClosed.cend())
	{
		return;
	}
	SocketNativeBufferT& nativeBuffer = thisReactor->*&HandleImplement::m_nativeBuffer;
	auto cacheIt = utils::dynamic_array_buffer::find<RawDataCache>(nativeBuffer, clientSocket);
	if (cacheIt == nativeBuffer.end())
	{
		return;
	}
	sharedLock.unlock();
	{
		std::unique_lock lock(thisReactor->*&HandleImplement::m_mutex);
		socketsToBeClosed.try_emplace(clientSocket, cacheIt);
	}
}
};

SocketReactor::SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port)
	: m_initType(i_initType), m_nativeHandle(nullptr)
	, m_workersConfig(utils::threadpool_config{i_initType == InitType::Connect ? k_clientWorkers : std::thread::hardware_concurrency()})
	, m_workerThreadpool(m_workersConfig)
	, m_socketData(i_socketAF, SOCK_STREAM, IPPROTO_TCP, i_address, i_port)
{
	if (i_socketAF < 0)
	{
		HandleError(Status::InitFailed, "Invalid socket address family!");
		return;
	}

	Socket_d nativeSocket = m_sockets.emplace<Socket>(this)->Open(i_socketAF, m_socketData.socketType, m_socketData.socketProtocol);
	if (nativeSocket == BS_INVALID_SOCKET)
	{
		m_sockets.pop_back();
		HandleError(Status::InitFailed, utils::Format("OpenSocket with error: {}", errno));
		return;
	}

	m_nativeHandle = malloc(sizeof(Socket_d));
	*(Socket_d*)m_nativeHandle = epoll_create1(0);
	if (m_nativeHandle == nullptr || *(Socket_d*)m_nativeHandle == BS_INVALID_SOCKET)
	{
		HandleError(Status::InitFailed, "creating epoll descriptor!");
		return;
	}

	m_status = Status::InitSuccess;
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
		}
	}
	std::unique_lock lock(m_mutex);
	m_sockets.erase(*i_socket);
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
	free(m_nativeHandle);
	utils::dynamic_array_buffer::deallocate<RawDataCache>(m_nativeBuffer);
	m_sockets.clear();
	auto eventIt = m_eventsMap.find(SocketEvent::ShuttingDown);
	if (eventIt == m_eventsMap.end())
	{
		return;
	}
	lock.unlock();
	utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&IEventHandler::HandleEvent, nullptr);
	lock.lock();
}

void SocketReactor::Update(float)
{
	{
		std::unique_lock lock(m_mutex);
		if (m_status == Status::Shuttingdown)
		{
			return;
		}
	}
	std::shared_lock sharedLock(m_mutex);
	const bool shouldCheckCloseClient = !m_socketsToBeClosed.empty();
	if (shouldCheckCloseClient)
	{
		utils::public_dynamic_buffer_iterator<RawDataCache, SocketNativeBufferT::buffer_t> dataCache = m_socketsToBeClosed.cbegin()->second;
		if (dataCache->writeData.begin == dataCache->writeData.end || dataCache->writeData.size == 0)
		{
			ISocket* obsoleteSocket = dataCache->socket;
			sharedLock.unlock();
			{
				std::unique_lock lock(m_mutex);
				m_socketsToBeClosed.erase(dataCache->socket->GetNativeSocket());
				m_nativeBuffer.erase(dataCache->selfIterator);
			}
			CloseClient(obsoleteSocket);
		}
	}
}

ISocketReactor::ReactorResult SocketReactor::ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket)
{
	if (i_socket == nullptr)
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "invalid socket in ProcessAsyncRawData");
	}
	std::shared_lock sharedLock(m_mutex);
	if (m_socketsToBeClosed.find(i_socket->GetNativeSocket()) != m_socketsToBeClosed.cend())
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "socket going to be closed!");
	}
	switch (i_eventType)
	{
	case SocketEvent::ReadStream:
	{
		// seems nothing to do
	}
	break;
	case SocketEvent::WriteStream:
	{
		using bytes_t = internal::data::WriteData::bytes_t;
		internal::data::WriteData& writeData = *reinterpret_cast<internal::data::WriteData*>(i_rawData);
		utils::public_dynamic_buffer_iterator<RawDataCache, SocketNativeBufferT::buffer_t> dataCache = utils::dynamic_array_buffer::find<RawDataCache>(m_nativeBuffer, i_socket);
		if (dataCache == m_nativeBuffer.end())
		{
			sharedLock.unlock();
			{
				std::unique_lock lock(m_mutex);
				dataCache = utils::dynamic_array_buffer::push<RawDataCache>(m_nativeBuffer, i_socket, bytes_t(writeData.begin, writeData.end), writeData);
				dataCache->selfIterator = dataCache;
			}
			sharedLock.lock();
			if (dataCache == m_nativeBuffer.end())
			{
				return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "allocate RawDataCache failed: {}");
			}
			dataCache->writeData.bytes = &dataCache->rawData;
			dataCache->writeData.begin = dataCache->rawData.begin();
			dataCache->writeData.end = dataCache->rawData.end();
		}
		else if (dataCache->writeData.size <= 0)
		{
			dataCache->rawData = bytes_t(writeData.begin, writeData.end);
			dataCache->writeData.bytes = &dataCache->rawData;
			dataCache->writeData.begin = dataCache->rawData.begin();
			dataCache->writeData.end = dataCache->rawData.end();
			dataCache->writeData.size = writeData.size;
		}
		else
		{
			dataCache->rawData.insert(dataCache->rawData.end(), writeData.begin, writeData.end);
			dataCache->writeData.end = dataCache->rawData.end();
			dataCache->writeData.size += writeData.size;
		}
		struct epoll_event ev;
		ev.data.fd = i_socket->GetNativeSocket();
		ev.events = EPOLLOUT | EPOLLET;
		if (epoll_ctl(*(Socket_d*)m_nativeHandle, EPOLL_CTL_MOD, i_socket->GetNativeSocket(), &ev) == -1)
		{
			HandleImplement::HandleCloseClient(this, *dataCache, sharedLock);
			return make_error<Error>(ErrorCode::InternalError, "epoll_ctl failed {}", errno);
		}
	}
	break;
	case SocketEvent::CloseConnection:
	{
		sharedLock.unlock();
		HandleImplement::HandleCloseSocket(this, i_socket->GetNativeSocket());
	}
	break;
	default: return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "Unhandled processing event");
	}

	return utils::Ok();
}

ISocketReactor::ReactorResult SocketReactor::Run()
{
	if (m_optError.has_value())
	{
		return m_optError.value();
	}

	struct addrinfo hints;
	struct addrinfo *result = NULL, *rp = NULL;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = m_socketData.socketAF;
	hints.ai_socktype = m_socketData.socketType;
	hints.ai_protocol = 0;

	utils::Epilogue clean([&result]()
	{
		if (result)
			freeaddrinfo(result);
	});

	switch (m_initType)
	{
	case InitType::Bind:
	{
		const int opt = 1;
		if(setsockopt(m_sockets.front().GetNativeSocket(), SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) == -1)
		{
			return make_error<Error>(ErrorCode::InternalError, "setsockopt failed {}", errno);
		}
		hints.ai_flags = AI_PASSIVE;
		hints.ai_addr = NULL;
		hints.ai_next = NULL;
		hints.ai_canonname = NULL;
		if (int r = getaddrinfo(m_socketData.address.empty() ? NULL : m_socketData.address.c_str(), std::to_string(m_socketData.port).c_str(), &hints, &result); r != 0)
		{
			return make_error<Error>(ErrorCode::InternalError, "getaddrinfo failed {}", gai_strerror(r));
		}
		std::shared_lock sharedLock(m_mutex);
		for (rp = result; rp != NULL; rp = rp->ai_next)
		{
			int r = bind(m_sockets.front().GetNativeSocket(), rp->ai_addr, (int)rp->ai_addrlen);
			if (r == 0 || errno == EINPROGRESS)
			{
				break;
			}
			m_sockets.pop_back();
			SocketsT::iterator socket = m_sockets.emplace<Socket>(this);
			Socket_d nativeSocket = socket->Open(m_socketData.socketAF, m_socketData.socketType, m_socketData.socketProtocol);
			if (nativeSocket == BS_INVALID_SOCKET)
			{
				rp = NULL;
				m_sockets.erase(socket);
				break;
			}
		}
		if (rp == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "bind failed {}", errno);
		}
		Socket_d server_fd = m_sockets.front().GetNativeSocket();
		fcntl(server_fd, F_SETFL, O_NONBLOCK);
		if (listen(server_fd, SOMAXCONN) != 0)
		{
			return make_error<Error>(ErrorCode::InternalError, "listen failed {}", errno);
		}

		sharedLock.unlock();
		{
			std::unique_lock lock(m_mutex);
			m_status = Status::Running;
		}
		sharedLock.lock();
		struct epoll_event ev;
		ev.data.fd = m_sockets.front().GetNativeSocket();
		ev.events = EPOLLIN | EPOLLET;
		if (epoll_ctl(*(Socket_d*)m_nativeHandle, EPOLL_CTL_ADD, server_fd, &ev) == -1)
		{
			return make_error<Error>(ErrorCode::InternalError, "epoll_ctl failed {}", errno);
		}

		struct epoll_event events[k_MaxEvents];
		while (m_status == Status::Running)
		{
			sharedLock.unlock();
			int nfds = epoll_wait(*(Socket_d*)m_nativeHandle, events, k_MaxEvents, k_waitInMs);
			if (nfds == -1 && errno != EINTR)
			{
				HandleError(Status::Shuttingdown, utils::Format("epoll_wait failed {}", errno));
				goto EOL;
			}
			for (int i = 0; i < nfds; i++)
			{
				const Socket_d socket_fd = events[i].data.fd;
				if (socket_fd == server_fd)
				{
					utils::async(m_workerThreadpool, HandleImplement::HandleAccept, this, server_fd);
				}
				else if((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP))
				{
					sharedLock.lock();
					auto socketIt = m_sockets.find_if([socket_fd](const ISocket& i_socket)
					{
						return i_socket.GetNativeSocket() == socket_fd;
					});
					if (socketIt == m_sockets.cend())
					{
						sharedLock.unlock();
						continue;
					}
					sharedLock.unlock();
					HandleImplement::HandleCloseSocket(this, socketIt->GetNativeSocket());
				}
				else if(events[i].events & EPOLLIN)
				{
					utils::async(m_workerThreadpool, HandleImplement::HandleReadStream, this, socket_fd);
				}
				else if(events[i].events & EPOLLOUT)
				{
					utils::async(m_workerThreadpool, HandleImplement::HandleWriteStream, this, socket_fd);
				}
			}

			EOL:
			sharedLock.lock();
		}
	}
	break;
	case InitType::Connect:
	{
		hints.ai_flags = 0;
		if (int r = getaddrinfo(m_socketData.address.empty() ? NULL : m_socketData.address.c_str(), std::to_string(m_socketData.port).c_str(), &hints, &result); r != 0)
		{
			return make_error<Error>(ErrorCode::InternalError, "getaddrinfo failed {}", gai_strerror(r));
		}
		std::shared_lock sharedLock(m_mutex);
		for (rp = result; rp != NULL; rp = rp->ai_next)
		{
			int r = connect(m_sockets.front().GetNativeSocket(), rp->ai_addr, (int)rp->ai_addrlen);
			if (r == 0 || errno == EINPROGRESS)
			{
				break;
			}
			m_sockets.pop_back();
			SocketsT::iterator socket = m_sockets.emplace<Socket>(this);
			Socket_d nativeSocket = socket->Open(m_socketData.socketAF, m_socketData.socketType, m_socketData.socketProtocol);
			if (nativeSocket == BS_INVALID_SOCKET)
			{
				rp = NULL;
				m_sockets.erase(socket);
				break;
			}
		}
		if (rp == NULL)
		{
			return make_error<Error>(ErrorCode::InternalError, "connect failed {}", errno);
		}
		Socket_d socket_fd = m_sockets.front().GetNativeSocket();
		fcntl(socket_fd, F_SETFL, O_NONBLOCK);
		struct epoll_event ev;
		ev.data.fd = m_sockets.front().GetNativeSocket();
		ev.events = EPOLLIN | EPOLLET;
		if (epoll_ctl(*(Socket_d*)m_nativeHandle, EPOLL_CTL_ADD, socket_fd, &ev) == -1)
		{
			return make_error<Error>(ErrorCode::InternalError, "epoll_ctl failed {}", errno);
		}
		m_status = Status::Running;

		using bytes_t = internal::data::WriteData::bytes_t;
		internal::data::WriteData writeData;
		utils::public_dynamic_buffer_iterator<RawDataCache, SocketNativeBufferT::buffer_t> dataCache
		= utils::dynamic_array_buffer::push<RawDataCache>(m_nativeBuffer, &m_sockets.front(), bytes_t(), writeData);
		dataCache->selfIterator = dataCache;
		dataCache->writeData.bytes = &dataCache->rawData;
		dataCache->writeData.begin = dataCache->writeData.end = dataCache->rawData.end();
		dataCache->writeData.size = 0;
		auto eventIt = m_eventsMap.find(SocketEvent::AcceptConnection);
		if (eventIt != m_eventsMap.end())
		{
			sharedLock.unlock();
			if (!utils::Access<AccessKey>(eventIt->second.cb_handleAction).Emit(&HandleImplement::HandleEvent, reinterpret_cast<void*>(&m_sockets.front())).value())
			{
				HandleImplement::HandleCloseClient(this, *dataCache, sharedLock);
				HandleError(Status::Shuttingdown, "Disconnected from server!");
			}
		}
		utils::async(m_workerThreadpool, [this, socket_fd]()
		{
			struct epoll_event events[1];
			std::shared_lock sharedLock(m_mutex);
			while (m_status == Status::Running)
			{
				if (m_sockets.empty())
				{
					HandleError(Status::Shuttingdown, "Disconnected from server!");
					return;
				}
				ISocket& socket = m_sockets.front();
				sharedLock.unlock();
				int nfds = epoll_wait(*(Socket_d*)m_nativeHandle, events, 1, k_waitInMs);
				if (nfds == -1 && errno != EINTR)
				{
					HandleImplement::HandleCloseSocket(this, socket.GetNativeSocket());
					HandleError(Status::Shuttingdown, utils::Format("epoll_wait failed {}", errno));
				}
				else if (nfds > 0)
				{
					if ((events[0].events & EPOLLERR) || (events[0].events & EPOLLHUP))
					{
						sharedLock.unlock();
						HandleImplement::HandleCloseSocket(this, socket.GetNativeSocket());
						HandleError(Status::Shuttingdown, "Disconnected from server!");
					}
					else if (events[0].events & EPOLLIN)
					{
						HandleImplement::HandleReadStream(this, socket_fd);
					}
					else if (events[0].events & EPOLLOUT)
					{
						HandleImplement::HandleWriteStream(this, socket_fd);
					}
				}
				sharedLock.lock();
			}
		});
	}
	break;
	}

	if (m_optError.has_value())
	{
		return m_optError.value();
	}
	return utils::Ok();
}

#endif