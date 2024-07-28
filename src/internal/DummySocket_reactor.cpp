#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"

#if defined(USE_DUMMY_API)

SocketReactor::SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port)
	: m_initType(i_initType), m_nativeHandle(nullptr)
	, m_workersConfig(utils::threadpool_config(i_initType == InitType::Connect ? 1 : std::thread::hardware_concurrency()))
	, m_workerThreadpool(m_workersConfig)
	, m_socketData(i_socketAF, 0, 0, i_address, i_port)
	, m_status(Status::InitFailed)
{
}

void SocketReactor::Update(float)
{
}

ISocketReactor::ReactorResult SocketReactor::Run()
{
	return make_error<Error>(ErrorCode::UnsupportedPlatform);
}

ISocketReactor::ReactorResult SocketReactor::ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket)
{
	return make_error<Error>(ErrorCode::UnsupportedPlatform);
}

void SocketReactor::Shutdown()
{
}

void SocketReactor::CloseClient(ISocket*)
{
}

#endif