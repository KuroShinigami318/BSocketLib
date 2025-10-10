#pragma once
#include "SocketEvent.h"
#include "dynamic_array_buffer.h"

namespace internal
{
struct WinIOOperation
{
	OVERLAPPED overlapped;
	SocketEvent eventType = SocketEvent::ReadStream;
	DWORD sentBytes = 0;
	DWORD totalBytes = 0;
	DWORD dwFlags = 0;
	WSABUF wsaBuffers;
	char buffers[DATA_BUFSIZE];
	std::vector<char> receivedBuffers;
	utils::dynamic_buffers<WinIOOperation>::iterator selfIterator;
	std::mutex mutex;

	WinIOOperation(SocketEvent i_eventType = SocketEvent::ReadStream, DWORD i_sentBytes = 0, DWORD i_totalBytes = 0)
		: eventType(i_eventType), sentBytes(i_sentBytes), totalBytes(i_totalBytes), selfIterator(utils::dynamic_buffers<WinIOOperation>::s_end())
	{
		wsaBuffers.buf = buffers;
		wsaBuffers.len = sizeof(buffers);
		SecureZeroMemory((PVOID)buffers, sizeof(buffers));
		overlapped.Internal = 0;
		overlapped.InternalHigh = 0;
		overlapped.Offset = 0;
		overlapped.OffsetHigh = 0;
		overlapped.hEvent = NULL;
	}

	WinIOOperation(const WinIOOperation& other)
		: selfIterator(other.selfIterator)
	{
		wsaBuffers.buf = buffers;
		wsaBuffers.len = sizeof(buffers);
		overlapped.Internal = 0;
		overlapped.InternalHigh = 0;
		overlapped.Offset = 0;
		overlapped.OffsetHigh = 0;
		overlapped.hEvent = NULL;
		eventType = other.eventType;
		sentBytes = other.sentBytes;
		totalBytes = other.totalBytes;
		dwFlags = other.dwFlags;

		memcpy(buffers, other.buffers, other.wsaBuffers.len);
	}
};
}