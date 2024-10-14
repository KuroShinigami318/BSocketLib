#pragma once
#include "SocketEvent.h"
#include "dynamic_array_buffer.h"

class ISocket;

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

	WinIOOperation(SocketEvent i_eventType = SocketEvent::ReadStream, DWORD i_sentBytes = 0, DWORD i_totalBytes = 0)
		: eventType(i_eventType), sentBytes(i_sentBytes), totalBytes(i_totalBytes)
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

struct WinIOContext
{
	ULONG_PTR key = 0ul;
	ISocket* socket = nullptr;
	utils::dynamic_buffers<WinIOOperation> ioOperations;

	bool operator==(const WinIOContext& other) const
	{
		return socket == other.socket && key == other.key;
	}

	WinIOContext(ULONG_PTR i_key = 0ul, ISocket* i_socket = nullptr, SocketEvent i_eventType = SocketEvent::ReadStream, DWORD i_sentBytes = 0, DWORD i_totalBytes = 0)
		: socket(i_socket)
		, key(i_key)
	{
		ioOperations.emplace(i_eventType, i_sentBytes, i_totalBytes);
	}
};
}