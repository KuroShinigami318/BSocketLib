#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"

#if defined(USE_POSIX_API)
#include <sys/socket.h>

SocketReactor::SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port)
	: m_initType(i_initType)
{

}

ISocketReactor::Result SocketReactor::Run()
{
	return Ok();
}

ISocketReactor::Result SocketReactor::RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler)
{
	return Ok();
}

ISocketReactor::Result SocketReactor::DeregisterEventHandler(SocketEvent i_event)
{
	return Ok();
}

#endif