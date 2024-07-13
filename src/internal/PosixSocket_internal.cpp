#include "stdafx.h"
#include "internal/Socket_internal.h"
#include "internal/data/WriteData.h"
#include "ISocket_reactor.h"

#if defined(USE_POSIX_API)
#include <errno.h>
#include <fcntl.h>
#include <sys/socket.h>

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
	m_socket_d = socket(i_socketAF, i_socketType, i_socketProtocol);
	return m_socket_d;
}

Socket_internal::SocketResult Socket_internal::Close()
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	if (close(m_socket_d) != 0)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError);
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
	if (recv(m_socket_d, bytes.data(), bytes.size(), 0) < 0)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "read failed with error: {}", errno);
	}
	return bytes;
}

Socket_internal::SocketResult Socket_internal::ReadBytesAsync(size_t i_size)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

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
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	ssize_t sResult = send(m_socket_d, i_bytes.data(), i_bytes.size(), 0);
	if (sResult < 0)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, "write failed with error: {}", errno);
	}
	return (size_t)sResult;
}

Socket_internal::SocketResult Socket_internal::WriteBytesAsync(BytesT& i_bytes)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	if (m_socketReactor == nullptr || m_isBlocking.load(std::memory_order_relaxed))
	{
		std::string error = m_socketReactor != nullptr ? "this function need socket reactor to process" : "currently blocking";
		return make_error<SocketError>(SocketErrorCode::InternalSocketError, error.c_str());
	}
	internal::data::WriteData writeData{&i_bytes, i_bytes.begin(), i_bytes.end(), i_bytes.size()};
	ISocketReactor::ReactorResult result = m_socketReactor->ProcessAsyncRawData(reinterpret_cast<void*>(&writeData), SocketEvent::WriteStream, m_selfInterface);
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