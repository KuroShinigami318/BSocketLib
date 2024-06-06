#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"

#if defined(USE_DUMMY_API)

SocketReactor::SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port)
	: m_initType(i_initType)
{
}

SocketReactor::~SocketReactor()
{
}

void SocketReactor::Update(float)
{
}

ISocketReactor::Result SocketReactor::Run()
{
	return make_error<Error>(ErrorCode::UnsupportedPlatform);
}

ISocketReactor::Result SocketReactor::RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler)
{
	return make_error<Error>(ErrorCode::UnsupportedPlatform);
}

ISocketReactor::Result SocketReactor::DeregisterEventHandler(SocketEvent i_event)
{
	return make_error<Error>(ErrorCode::UnsupportedPlatform);
}

ISocketReactor::Result SocketReactor::ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket)
{
	return make_error<Error>(ErrorCode::UnsupportedPlatform);
}

void SocketReactor::Shutdown()
{
}

void SocketReactor::CloseClient(ISocket*)
{
}

void SocketReactor::HandleError(Status i_status, std::string i_locationFailed)
{
}

#endif