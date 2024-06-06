#pragma once

#include "Socket_d.h"
#include "SocketError.h"
#include <vector>

class ISocket
{
public:
	using BytesT = std::vector<char>;
	using ReadResult = Result<BytesT, SocketError>;
	using WriteResult = Result<size_t, SocketError>;
	using Result = Result<void, SocketError>;
public:
	virtual Socket_d GetNativeSocket() const = 0;
	virtual Socket_d Open(Socket_AF, Socket_Type, Socket_Protocol) = 0;
	virtual bool Close() = 0;
	virtual ReadResult ReadBytes(size_t i_size) = 0;
	virtual Result ReadBytesAsync(size_t i_size) = 0;
	virtual WriteResult WriteBytes(BytesT& i_bytes) = 0;
	virtual Result WriteBytesAsync(BytesT& i_bytes) = 0;
	virtual ~ISocket() = default;
};