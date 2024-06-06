#include "stdafx.h"
#include "internal/Socket_internal.h"
#include "ISocket_reactor.h"

#if defined(USE_DUMMY_API)

Socket_internal::Socket_internal(ISocketReactor* i_socketReactor, Socket_d i_socket_d)
	: m_socketReactor(i_socketReactor), m_socket_d(i_socket_d)
{
}

Socket_d Socket_internal::Open(Socket_AF i_socketAF, Socket_Type i_socketType, Socket_Protocol i_socketProtocol)
{
	return m_socket_d;
}

Socket_internal::SocketResult Socket_internal::Close()
{
	if (m_socket_d == 0)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return false;
}

Socket_internal::ReadResult Socket_internal::ReadBytes(size_t i_size)
{
	if (m_socket_d == 0)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return BytesT();
}

Socket_internal::Result Socket_internal::ReadBytesAsync(size_t i_size)
{
	if (m_socket_d == 0)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return Ok();
}

Socket_internal::WriteResult Socket_internal::WriteBytes(BytesT& i_bytes)
{
	if (m_socket_d == 0)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return 0;
}

Socket_internal::Result Socket_internal::WriteBytesAsync(BytesT& i_bytes)
{
	if (m_socket_d == 0)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return Ok();
}

bool operator==(const Socket_internal& lhs, const Socket_internal& rhs)
{
	return lhs.m_socket_d == rhs.m_socket_d;
}

#endif