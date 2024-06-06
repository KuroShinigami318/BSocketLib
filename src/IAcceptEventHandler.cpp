#include "stdafx.h"
#include "IAcceptEventHandler.h"
#include "ISocket.h"

SocketEvent IAcceptEventHandler::GetEventType() const
{
	return SocketEvent::AcceptConnection;
}

bool IAcceptEventHandler::HandleEvent(void* i_rawData)
{
	ISocket& socket = *reinterpret_cast<ISocket*>(i_rawData);
	return HandleAcceptEvent(socket);
}