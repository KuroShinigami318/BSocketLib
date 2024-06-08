#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"

#if defined(USE_POSIX_API)
#include <sys/socket.h>

SocketReactor::EventData::EventData(std::unique_ptr<IEventHandler> i_eventHandler)
	: eventHandler(std::move(i_eventHandler))
{
}

SocketReactor::SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port)
	: m_initType(i_initType), m_nativeHandle(nullptr)
	, m_workersConfig(utils::threadpool_config(i_initType == InitType::Connect ? 1 : std::thread::hardware_concurrency()))
	, m_workerThreadpool(m_workersConfig)
	, m_socketData(i_socketAF, SOCK_STREAM, 0, i_address, i_port)
{
	if (i_socketAF < 0)
	{
		HandleError(Status::InitFailed, "Invalid socket address family!");
		return;
	}
}

void SocketReactor::HandleError(Status i_status, std::string i_locationFailed)
{
	std::unique_lock lock(m_mutex);
	m_status = i_status;
	m_optError = make_error<Error>(ErrorCode::InternalError, "failed in {}", i_locationFailed);
}

void SocketReactor::CloseClient(ISocket* i_socket)
{

}

SocketReactor::~SocketReactor()
{
	Shutdown();
}

void SocketReactor::Shutdown()
{

}

void SocketReactor::Update(float)
{

}

ISocketReactor::ReactorResult SocketReactor::ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket)
{
	if (i_socket == nullptr)
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "invalid socket in ProcessAsyncRawData");
	}
	std::shared_lock sharedLock(m_mutex);
	if (std::find(m_socketsToBeClosed.cbegin(), m_socketsToBeClosed.cend(), i_socket) != m_socketsToBeClosed.cend())
	{
		return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "socket going to be closed!");
	}
	switch (i_eventType)
	{
	case SocketEvent::ReadStream:
	{
	}
	break;
	case SocketEvent::WriteStream:
	{
	}
	break;
	default: return make_error<ISocketReactor::Error>(ISocketReactor::ErrorCode::InternalError, "Unhandled processing event");
	}

	return Ok();
}

ISocketReactor::ReactorResult SocketReactor::Run()
{
	if (m_optError.has_value())
	{
		return m_optError.value();
	}

	return Ok();
}

ISocketReactor::ReactorResult SocketReactor::RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler)
{
	std::unique_lock lock(m_mutex);
	if (m_eventsMap.find(i_event) != m_eventsMap.end())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event has already been registered!", i_event);
	}
	if (i_eventHandler == nullptr || i_event != i_eventHandler->GetEventType())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event mismatch with the given handler!", i_event);
	}
	auto eventMap = m_eventsMap.try_emplace(i_event, std::move(i_eventHandler));
	EventData& eventData = eventMap.first->second;
	IEventHandler* eventHandler = eventData.eventHandler.get();
	eventHandler->m_connection = eventMap.first->second.cb_handleAction.Connect(eventHandler);

	return Ok();
}

ISocketReactor::ReactorResult SocketReactor::DeregisterEventHandler(SocketEvent i_event)
{
	std::unique_lock lock(m_mutex);
	if (m_eventsMap.find(i_event) == m_eventsMap.end())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event is not registered!", i_event);
	}
	m_eventsMap.erase(i_event);
	return Ok();
}

#endif