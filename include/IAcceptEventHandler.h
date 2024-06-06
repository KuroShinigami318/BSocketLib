#pragma once

#include "IEventHandler.h"

class ISocket;

class IAcceptEventHandler : public IEventHandler
{
public:
	virtual bool HandleAcceptEvent(ISocket& i_socket) = 0;
	SocketEvent GetEventType() const override;

protected:
	bool HandleEvent(void* i_rawData) override;
};