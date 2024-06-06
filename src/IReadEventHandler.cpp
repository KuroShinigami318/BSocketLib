#include "stdafx.h"
#include "IReadEventHandler.h"
#include "internal/data/ReadData.h"

SocketEvent IReadEventHandler::GetEventType() const
{
	return SocketEvent::ReadStream;
}

bool IReadEventHandler::HandleEvent(void* i_rawData)
{
	internal::data::ReadData& readData = *reinterpret_cast<internal::data::ReadData*>(i_rawData);
	return HandleReadEvent(readData.bytes, readData.socket);
}