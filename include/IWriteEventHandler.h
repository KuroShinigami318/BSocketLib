#pragma once

#include "IEventHandler.h"

class IWriteEventHandler : public IEventHandler
{
public:
	virtual bool HandleWriteEvent(const size_t& i_bytesSent) = 0;
	SocketEvent GetEventType() const override;

protected:
	bool HandleEvent(void* i_rawData) override;
};