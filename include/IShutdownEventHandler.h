#pragma once

#include "IEventHandler.h"

class IShutdownEventHandler : public IEventHandler
{
public:
	virtual bool HandleShutdownEvent() = 0;
	SocketEvent GetEventType() const override;

protected:
	bool HandleEvent(void* i_rawData) override;
};