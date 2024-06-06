#include "stdafx.h"
#include "IWriteEventHandler.h"

SocketEvent IWriteEventHandler::GetEventType() const
{
	return SocketEvent::WriteStream;
}

bool IWriteEventHandler::HandleEvent(void* i_rawData)
{
	size_t& bytesSent = *reinterpret_cast<size_t*>(i_rawData);
	return HandleWriteEvent(bytesSent);
}