#include "stdafx.h"
#include "ICloseConnectionEventHandler.h"
#include "ISocket.h"

SocketEvent ICloseConnectionEventHandler::GetEventType() const
{
	return SocketEvent::CloseConnection;
}

bool ICloseConnectionEventHandler::HandleEvent(void* i_rawData)
{
	ISocket& socket = *reinterpret_cast<ISocket*>(i_rawData);
	return HandleCloseEvent(socket);
}