#pragma once

DeclareScopedEnumWithOperatorDefined(SocketEvent, DUMMY_NAMESPACE, uint32_t
	, AcceptConnection
	, CloseConnection
	, ShuttingDown
	, ReadStream
	, WriteStream);