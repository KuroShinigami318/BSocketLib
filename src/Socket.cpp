#include "Socket.h"

Socket::Socket(ISocketReactor* i_socketReactor, Socket_d i_socket_d)
	: m_socketInternal(this, i_socketReactor, i_socket_d)
{
}

Socket::~Socket()
{
	Close();
}

std::string Socket::GetIPAddress() const
{
	return m_socketInternal.GetIPAddressSocket();
}

Socket_d Socket::GetNativeSocket() const
{
	return m_socketInternal.GetNativeSocket();
}

Socket_d Socket::Open(Socket_AF i_socketAF, Socket_Type i_socketType, Socket_Protocol i_socketProtocol)
{
	return m_socketInternal.Open(i_socketAF, i_socketType, i_socketProtocol);
}

bool Socket::Close()
{
	return m_socketInternal.Close().isOk() ? true : false;
}

ISocket::ReadResult Socket::ReadBytes(size_t i_size)
{
	Socket_internal::ReadResult readResult = m_socketInternal.ReadBytes(i_size);
	if (readResult.isErr())
	{
		return readResult.unwrapErr();
	}
	return readResult.unwrap();
}

ISocket::SocketResult Socket::ReadBytesAsync(size_t i_size)
{
	Socket_internal::SocketResult readResult = m_socketInternal.ReadBytesAsync(i_size);
	if (readResult.isErr())
	{
		return readResult.unwrapErr();
	}
	return utils::Ok();
}

ISocket::WriteResult Socket::WriteBytes(BytesT& i_bytes)
{
	Socket_internal::WriteResult writeResult = m_socketInternal.WriteBytes(i_bytes);
	if (writeResult.isErr())
	{
		return writeResult.unwrapErr();
	}
	return writeResult.unwrap();
}

ISocket::SocketResult Socket::WriteBytesAsync(BytesT& i_bytes)
{
	Socket_internal::SocketResult writeResult = m_socketInternal.WriteBytesAsync(i_bytes);
	if (writeResult.isErr())
	{
		return writeResult.unwrapErr();
	}
	return utils::Ok();
}

void Socket::SetBlockProcess(bool i_blocking)
{
	m_socketInternal.SetBlockProcess(i_blocking);
}

bool Socket::IsBlockProcess() const
{
	return m_socketInternal.IsBlockProcess();
}

bool operator==(const Socket& lhs, const Socket& rhs)
{
	return lhs.m_socketInternal == rhs.m_socketInternal;
}

bool operator!=(const Socket& lhs, const Socket& rhs)
{
	return !(lhs == rhs);
}