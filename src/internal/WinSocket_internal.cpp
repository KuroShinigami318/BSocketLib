#include "stdafx.h"
#include "internal/Socket_internal.h"
#include "ISocket_reactor.h"

#if defined(USE_WIN32_API)
#include <WinSock2.h>
#include "internal/WinIOContext.h"

Socket_internal::Socket_internal(ISocket* i_selfInterface, ISocketReactor* i_socketReactor, Socket_d i_socket_d)
	: m_selfInterface(i_selfInterface), m_socketReactor(i_socketReactor), m_socket_d(i_socket_d)
{
}

Socket_d Socket_internal::GetNativeSocket() const
{
	return m_socket_d;
}

Socket_d Socket_internal::Open(Socket_AF i_socketAF, Socket_Type i_socketType, Socket_Protocol i_socketProtocol)
{
	if (m_socket_d != BS_INVALID_SOCKET)
	{
		return m_socket_d;
	}
	m_socket_d = WSASocket(i_socketAF, i_socketType, i_socketProtocol, NULL, 0, WSA_FLAG_OVERLAPPED);
	return m_socket_d;
}

Socket_internal::SocketResult Socket_internal::Close()
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	shutdown(m_socket_d, SD_BOTH);
	if (closesocket(m_socket_d) != 0)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "closesocket failed with error: {}", WSAGetLastError());
	}
	m_socket_d = BS_INVALID_SOCKET;

	return Ok();
}

Socket_internal::ReadResult Socket_internal::ReadBytes(size_t i_size)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}
	BytesT bytes(i_size);
	internal::WinIOOperation ioOperation;
	ioOperation.eventType = SocketEvent::ReadStream;
	ioOperation.overlapped.hEvent = WSACreateEvent();
	if (ioOperation.overlapped.hEvent == NULL)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSACreateEvent failed with error: {}", WSAGetLastError());
	}
	utils::Epilogue cleanup([&ioOperation]() { WSACloseEvent(ioOperation.overlapped.hEvent); });

	WSABUF DataBuf{};
	DWORD RecvBytes{};
	DWORD Flags{};
	DataBuf.len = bytes.size();
	DataBuf.buf = bytes.data();
	int rc = WSARecv(m_socket_d, &DataBuf, 1, &RecvBytes, &Flags, (LPWSAOVERLAPPED) &ioOperation, NULL);
	int err = 0;
	if ((rc == SOCKET_ERROR) &&
		(WSA_IO_PENDING != (err = WSAGetLastError()))) {
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSARecv failed with error: {}", err);
	}

	rc = WSAWaitForMultipleEvents(1, &ioOperation.overlapped.hEvent, TRUE, INFINITE, TRUE);
	if (rc == WSA_WAIT_FAILED)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSAWaitForMultipleEvents failed with error: {}", WSAGetLastError());
	}
	rc = WSAGetOverlappedResult(m_socket_d, (LPWSAOVERLAPPED) &ioOperation, &RecvBytes, FALSE, &Flags);
	if (rc == FALSE)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSARecv failed with error: {}", WSAGetLastError());
	}
	return bytes;
}

Socket_internal::SocketResult Socket_internal::ReadBytesAsync(size_t i_size)
{
	if (m_socketReactor == nullptr || m_isBlocking.load(std::memory_order_relaxed))
	{
		std::string error = m_socketReactor != nullptr ? "this function need socket reactor to process" : "currently blocking";
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, error.c_str());
	}
	ISocketReactor::ReactorResult result = m_socketReactor->ProcessAsyncRawData(reinterpret_cast<void*>(&i_size), SocketEvent::ReadStream, m_selfInterface);
	if (result.isErr())
	{
		return make_inner_error<SocketError>(SocketErrorCode::InternalSocketError, result.unwrapErr());
	}
	return Ok();
}

Socket_internal::WriteResult Socket_internal::WriteBytes(BytesT& i_bytes)
{
	if (m_socket_d == INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}
	internal::WinIOOperation ioOperation;
	ioOperation.eventType = SocketEvent::WriteStream;
	ioOperation.overlapped.hEvent = WSACreateEvent();
	if (ioOperation.overlapped.hEvent == NULL)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSACreateEvent failed with error: {}", WSAGetLastError());
	}
	utils::Epilogue cleanup([&ioOperation]() { WSACloseEvent(ioOperation.overlapped.hEvent); });
	WSABUF DataBuf{};
	DWORD SendBytes{};
	DWORD Flags{};
	DataBuf.len = i_bytes.size();
	DataBuf.buf = i_bytes.data();
	int rc = WSASend(m_socket_d, &DataBuf, 1, &SendBytes, 0, (LPWSAOVERLAPPED) &ioOperation, NULL);
	int err = 0;
	if ((rc == SOCKET_ERROR) &&
		(WSA_IO_PENDING != (err = WSAGetLastError()))) {
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSASend failed with error: {}", err);
	}

	rc = WSAWaitForMultipleEvents(1, &ioOperation.overlapped.hEvent, TRUE, INFINITE, TRUE);
	if (rc == WSA_WAIT_FAILED)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSAWaitForMultipleEvents failed with error: {}", WSAGetLastError());
	}
	rc = WSAGetOverlappedResult(m_socket_d, (LPWSAOVERLAPPED) &ioOperation, &SendBytes, FALSE, &Flags);
	if (rc == FALSE)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "WSASend failed with error: {}", WSAGetLastError());
	}
	return (size_t) SendBytes;
}

Socket_internal::SocketResult Socket_internal::WriteBytesAsync(BytesT& i_bytes)
{
	if (m_socketReactor == nullptr || m_isBlocking.load(std::memory_order_relaxed))
	{
		std::string error = m_socketReactor != nullptr ? "this function need socket reactor to process" : "currently blocking";
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, error.c_str());
	}
	ISocketReactor::ReactorResult result = m_socketReactor->ProcessAsyncRawData(reinterpret_cast<void*>(&i_bytes), SocketEvent::WriteStream, m_selfInterface);
	if (result.isErr())
	{
		return make_inner_error<SocketError>(SocketErrorCode::InternalSocketError, result.unwrapErr());
	}
	return Ok();
}

void Socket_internal::SetBlockProcess(bool i_blocking)
{
	m_isBlocking.store(i_blocking, std::memory_order_relaxed);
}

bool operator==(const Socket_internal& lhs, const Socket_internal& rhs)
{
	return lhs.m_socket_d == rhs.m_socket_d;
}

#endif