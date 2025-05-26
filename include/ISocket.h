#pragma once

#include "Socket_d.h"
#include "SocketError.h"
#include "intrusive_base.h"
#include <vector>

class SocketReactor;

class ISocket : public utils::intrusive_base
{
public:
	using BytesT = std::vector<char>;
	using ReadResult = utils::Result<BytesT, SocketError>;
	using WriteResult = utils::Result<size_t, SocketError>;
	using SocketResult = utils::Result<void, SocketError>;
public:
	virtual Socket_d GetNativeSocket() const = 0;
	virtual Socket_d Open(Socket_AF, Socket_Type, Socket_Protocol) = 0;
	virtual bool Close() = 0;
	virtual ReadResult ReadBytes(size_t i_size) = 0;
	virtual SocketResult ReadBytesAsync(size_t i_size) = 0;
	virtual WriteResult WriteBytes(BytesT& i_bytes) = 0;
	virtual SocketResult WriteBytesAsync(BytesT& i_bytes) = 0;
	virtual ~ISocket() = default;

protected:
	friend class SocketReactor;
	virtual void SetBlockProcess(bool i_blocking) = 0;
	virtual bool IsBlockProcess() const = 0;
};