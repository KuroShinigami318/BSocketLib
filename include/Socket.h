#pragma once

#include "ISocket.h"
#include "internal/Socket_internal.h"

class ISocketReactor;

class Socket : public ISocket
{
public:
	Socket(ISocketReactor* i_socketReactor = nullptr, Socket_d i_socket_d = BS_INVALID_SOCKET);
	~Socket();
	std::string GetIPAddress() const override;
	Socket_d GetNativeSocket() const override;
	Socket_d Open(Socket_AF, Socket_Type, Socket_Protocol) override;
	bool Close() override;
	ReadResult ReadBytes(size_t i_size) override;
	SocketResult ReadBytesAsync(size_t i_size) override;
	WriteResult WriteBytes(BytesT& i_bytes) override;
	SocketResult WriteBytesAsync(BytesT& i_bytes) override;

private:
	void SetBlockProcess(bool i_blocking) override;
	bool IsBlockProcess() const override;;
	friend bool operator==(const Socket& lhs, const Socket& rhs);
	Socket_internal m_socketInternal;
};

bool operator==(const Socket& lhs, const Socket& rhs);
bool operator!=(const Socket& lhs, const Socket& rhs);