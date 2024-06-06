#include "stdafx.h"
#include "internal/Socket_internal.h"
#include "ISocket_reactor.h"

#if defined(USE_POSIX_API)
#include <sys/socket.h>

Socket_internal::Socket_internal(ISocketReactor* i_socketReactor, Socket_d i_socket_d)
	: m_socketReactor(i_socketReactor), m_socket_d(i_socket_d)
{
}

Socket_d Socket_internal::Open(Socket_AF i_socketAF, Socket_Type i_socketType, Socket_Protocol i_socketProtocol)
{
	m_socket_d = socket(i_socketAF, i_socketType, i_socketProtocol);
	return m_socket_d;
}

Socket_internal::SocketResult Socket_internal::Close()
{
	if (m_socket_d == 0)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	if (close(m_socket_d) != 0)
	{
		return make_error<SocketError>(SocketErrorCode::InternalSocketError);
	}
	m_socket_d = 0;
	return true;
}

bool operator==(const Socket_internal& lhs, const Socket_internal& rhs)
{
	return lhs.m_socket_d == rhs.m_socket_d;
}

#endif