#pragma once

#include "IEventHandler.h"
#include <vector>

class ISocket;

class IReadEventHandler : public IEventHandler
{
public:
	virtual bool HandleReadEvent(const std::vector<char>& i_bytes, ISocket& i_socket) = 0;
	SocketEvent GetEventType() const override;

protected:
	bool HandleEvent(void* i_rawData) override;
};