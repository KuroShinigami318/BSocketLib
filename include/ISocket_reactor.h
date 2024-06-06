#pragma once

#include "Socket_d.h"
#include "SocketEvent.h"

class IEventHandler;
class ISocket;

class ISocketReactor
{
public:
	DeclareScopedEnum(ErrorCode, uint32_t, InternalError, UnsupportedPlatform);
	using Error = utils::Error<ErrorCode>;
	using Result = Result<void, Error>;
public:
	virtual ~ISocketReactor() = default;
	virtual ISocketReactor::Result RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler) = 0;
	virtual ISocketReactor::Result DeregisterEventHandler(SocketEvent i_event) = 0;
private:
	friend class Socket_internal;
	virtual ISocketReactor::Result ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket) = 0;
};

DefineScopeEnumOperatorImpl(ErrorCode, ISocketReactor);