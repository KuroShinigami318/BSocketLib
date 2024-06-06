#pragma once
#include "ISocket_reactor.h"

DeclareScopedEnumWithOperatorDefined(SocketErrorCode, DUMMY_NAMESPACE, uint16_t
	, InternalSocketError
	, SocketClosed
	, InvalidSocket);
using SocketError = utils::Error<SocketErrorCode, ISocketReactor::Error>;