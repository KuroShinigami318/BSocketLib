#pragma once

#include "ISocket_reactor.h"
#include "ISocket.h"
#include "dynamic_array_buffer.h"

using SocketNativeBufferT = utils::dynamic_buffers<uint8_t>;
using SocketMapT = std::unordered_map<Socket_d, SocketNativeBufferT::iterator>;
using SocketsT = std::unordered_map<Socket_d, utils::unique_ref<ISocket>>;

class SocketReactor : public ISocketReactor
{
public:
	DeclareScopedEnum(InitType, uint8_t, Bind, Connect);
	DeclareInnerScopedEnum(Status, uint8_t, InitFailed, InitSuccess, Running, Shuttingdown, Shutdowned);
public:
	SocketReactor(InitType i_initType, Socket_AF i_socketAF, std::string i_address, PORT i_port);
	~SocketReactor();
	void Update(float);
	ISocketReactor::ReactorResult Run();
	Status GetStatus() const;
	void Shutdown();
	ISocketReactor::ReactorResult RegisterEventHandler(SocketEvent i_event, std::unique_ptr<IEventHandler> i_eventHandler) override;
	ISocketReactor::ReactorResult DeregisterEventHandler(SocketEvent i_event) override;

protected:
	struct AccessKey;
	struct EventData
	{
		EventData(std::unique_ptr<IEventHandler> i_eventHandler);
		std::unique_ptr<IEventHandler> eventHandler;
		utils::Callback_public_mt<IEventHandler, AccessKey> cb_handleAction;
	};
	struct SocketData
	{
		SocketData(Socket_AF i_socketAF, Socket_Type i_socketType, Socket_Protocol i_socketProtocol, std::string i_address, PORT i_port)
			: socketAF(i_socketAF), socketType(i_socketType), socketProtocol(i_socketProtocol), address(i_address), port(i_port)
		{
		}
		Socket_AF socketAF;
		Socket_Type socketType;
		Socket_Protocol socketProtocol;
		std::string address;
		PORT port;
	};

protected:
	ISocketReactor::ReactorResult ProcessAsyncRawData(void* i_rawData, SocketEvent i_eventType, ISocket* i_socket) override;
	void CloseClient(ISocket* i_socket);
	void HandleError(Status i_status, std::string i_locationFailed);

	SocketData m_socketData;
	void* m_nativeHandle;
	SocketNativeBufferT m_nativeBuffer;
	InitType m_initType;
	Status m_status;
	std::optional<Error> m_optError;
	std::vector<utils::async_waitable<void>> m_waitables;
	std::unique_ptr<ISocket> m_hostSocket;
	SocketsT m_sockets;
	SocketMapT m_socketsMappedNativeContexts;
	SocketMapT m_socketsToBeClosed;
	std::unordered_map<SocketEvent, EventData> m_eventsMap;
	mutable std::shared_mutex m_mutex;
	utils::MessageSink_mt m_messageQueue;
	utils::threadpool_config m_workersConfig;
	utils::message_threadpool m_workerThreadpool;
};

DefineScopeEnumOperatorImpl(InitType, SocketReactor);
DefineScopeEnumOperatorImpl(Status, SocketReactor);