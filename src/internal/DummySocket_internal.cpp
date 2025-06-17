#include "stdafx.h"
#include "internal/Socket_internal.h"
#include "ISocket_reactor.h"

#if defined(USE_DUMMY_API)

Socket_internal::Socket_internal(ISocket* i_selfInterface, ISocketReactor* i_socketReactor, Socket_d i_socket_d)
	: m_selfInterface(i_selfInterface), m_socketReactor(i_socketReactor), m_socket_d(i_socket_d)
{
}

Socket_d Socket_internal::Open(Socket_AF i_socketAF, Socket_Type i_socketType, Socket_Protocol i_socketProtocol)
{
	return m_socket_d;
}

std::string Socket_internal::GetIPAddressSocket() const
{
	return "";
}

Socket_d Socket_internal::GetNativeSocket() const
{
	return m_socket_d;
}

Socket_internal::SocketResult Socket_internal::Close()
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return utils::Ok();
}

Socket_internal::ReadResult Socket_internal::ReadBytes(size_t i_size)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return BytesT();
}

Socket_internal::SocketResult Socket_internal::ReadBytesAsync(size_t i_size)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return utils::Ok();
}

Socket_internal::WriteResult Socket_internal::WriteBytes(BytesT& i_bytes)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return (size_t)0;
}

Socket_internal::SocketResult Socket_internal::WriteBytesAsync(BytesT& i_bytes)
{
	if (m_socket_d == BS_INVALID_SOCKET)
	{
		return make_error<SocketError>(SocketErrorCode::InvalidSocket);
	}

	return utils::Ok();
}

void Socket_internal::SetBlockProcess(bool i_blocking)
{

}

bool Socket_internal::IsBlockProcess() const
{
	return true;
}

bool operator==(const Socket_internal& lhs, const Socket_internal& rhs)
{
	return lhs.m_socket_d == rhs.m_socket_d;
}

#endif