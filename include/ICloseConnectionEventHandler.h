#pragma once

#include "IEventHandler.h"

class ISocket;

class ICloseConnectionEventHandler : public IEventHandler
{
public:
	virtual bool HandleCloseEvent(ISocket& i_socket) = 0;
	SocketEvent GetEventType() const override;

protected:
	bool HandleEvent(void* i_rawData) override;
};