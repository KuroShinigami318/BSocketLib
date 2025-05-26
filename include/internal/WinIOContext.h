#pragma once
#include "WinIOOperation.h"

class ISocket;

namespace internal
{
struct WinIOContext;
}

using public_buffer_iterator = utils::public_dynamic_buffer_iterator<internal::WinIOContext, SocketNativeBufferT::buffer_t>;

namespace internal
{
struct WinIOContext
{
	ULONG_PTR key = 0ul;
	ISocket* socket = nullptr;
	utils::dynamic_buffers<WinIOOperation> ioOperations;
	public_buffer_iterator selfIterator;

	bool operator==(const WinIOContext& other) const
	{
		return socket == other.socket && key == other.key;
	}

	WinIOContext(ULONG_PTR i_key = 0ul, ISocket* i_socket = nullptr, SocketEvent i_eventType = SocketEvent::ReadStream, DWORD i_sentBytes = 0, DWORD i_totalBytes = 0)
		: socket(i_socket)
		, key(i_key)
		, selfIterator(SocketNativeBufferT::s_end())
	{
		auto ioOperatonIt = ioOperations.emplace(i_eventType, i_sentBytes, i_totalBytes);
		ioOperatonIt->selfIterator = ioOperatonIt;
	}
};
}