#include "stdafx.h"
#include "Socket_reactor.h"
#include "IEventHandler.h"

SocketReactor::EventData::EventData(std::unique_ptr<IEventHandler> i_eventHandler)
	: eventHandler(std::move(i_eventHandler))
{
}

SocketReactor::~SocketReactor()
{
	std::shared_lock lock(m_mutex);
	while (m_status == Status::Shuttingdown)
	{
		lock.unlock();
		utils::this_thread::yield();
		utils::this_thread::sleep_for(utils::milisecs(1));
		lock.lock();
	}
	if (m_status != Status::Shutdowned)
	{
		lock.unlock();
		Shutdown();
		lock.lock();
	}
}

void SocketReactor::HandleError(Status i_status, std::string i_locationFailed)
{
	std::unique_lock lock(m_mutex);
	m_status = i_status;
	m_optError = make_error<Error>(ErrorCode::InternalError, "failed in {}", i_locationFailed);
}

ISocketReactor::ReactorResult SocketReactor::RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler)
{
	std::unique_lock lock(m_mutex);
	if (!utils::Contains(m_status, {Status::InitSuccess, Status::Running}))
	{
		return make_error<Error>(ErrorCode::InternalError, "Couldn't do in current status {}", m_status);
	}
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
	if (!utils::Contains(m_status, { Status::InitSuccess, Status::Running }))
	{
		return make_error<Error>(ErrorCode::InternalError, "Couldn't do in current status {}", m_status);
	}
	if (m_eventsMap.find(i_event) == m_eventsMap.end())
	{
		return make_error<Error>(ErrorCode::InternalError, "{} event is not registered!", i_event);
	}
	m_eventsMap.erase(i_event);
	return Ok();
}