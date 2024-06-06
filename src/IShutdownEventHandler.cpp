#include "stdafx.h"
#include "IShutdownEventHandler.h"

SocketEvent IShutdownEventHandler::GetEventType() const
{
	return SocketEvent::ShuttingDown;
}

bool IShutdownEventHandler::HandleEvent(void* i_rawData)
{
	return HandleShutdownEvent();
}