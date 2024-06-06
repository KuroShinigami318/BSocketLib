#pragma once

#include "SocketEvent.h"

class IEventHandler
{
public:
	virtual SocketEvent GetEventType() const = 0;
	virtual ~IEventHandler() = default;

protected:
	virtual bool HandleEvent(void*) = 0;

private:
	friend class SocketReactor;
	utils::Connection m_connection;
};

namespace internal
{
namespace data
{
struct ReadData;
}
}