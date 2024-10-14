#pragma once

#include "Socket_d.h"
#include "SocketError.h"
#include <vector>

class ISocket;
class ISocketReactor;

class Socket_internal
{
public:
	using BytesT = std::vector<char>;
	using ReadResult = Result<BytesT, SocketError>;
	using WriteResult = Result<size_t, SocketError>;
	using SocketResult = Result<void, SocketError>;
public:
	Socket_internal(ISocket* i_selfInterface, ISocketReactor* i_socketReactor = nullptr, Socket_d i_socket_d = BS_INVALID_SOCKET);
	Socket_d GetNativeSocket() const;
	Socket_d Open(Socket_AF, Socket_Type, Socket_Protocol);
	SocketResult Close();
	ReadResult ReadBytes(size_t i_size);
	SocketResult ReadBytesAsync(size_t i_size);
	WriteResult WriteBytes(BytesT& i_bytes);
	SocketResult WriteBytesAsync(BytesT& i_bytes);
	void SetBlockProcess(bool i_blocking);
	bool IsBlockProcess() const;

private:
	friend bool operator==(const Socket_internal& lhs, const Socket_internal& rhs);
	Socket_d m_socket_d = Socket_d(0);
	ISocket* m_selfInterface = nullptr;
	ISocketReactor* m_socketReactor = nullptr;
	std::atomic_bool m_isBlocking = false;
};

bool operator==(const Socket_internal& lhs, const Socket_internal& rhs);