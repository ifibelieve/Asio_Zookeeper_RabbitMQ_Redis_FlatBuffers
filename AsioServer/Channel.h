#pragma once
#include <memory>
#include <mutex>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <hiredis/hiredis.h>

#include "TalkToClient.h"
#include "../Zookeeper/ZkHandler.h"
#include "MQHandler.h"

namespace BaeServer
{
	using namespace boost::asio;

	class Channel :
		public std::enable_shared_from_this<Channel>
	{
		using self_type	= Channel;
		using container = std::map<TalkToClient::id_type, TalkToClient::ptr>;
		using type_lock = std::unique_lock<std::mutex>;
		using type_mutex = std::mutex;
		//using type_lock = boost::recursive_mutex::scoped_lock;
		//using type_mutex = boost::recursive_mutex;
		
		const size_t CHANNEL_CAPACITY = 2;
		
#define MEM_FN3(x,y,z)	boost::bind(&self_type::x, shared_from_this(), y, z)

		Channel(unsigned short port) :
			ep_{ ip::tcp::v4(), port },
			ac_{ service_, ep_ },
			started_{ false } { Init(); }
	public:
		~Channel();
		
		using id_type = unsigned long long;
		using ptr = std::shared_ptr<self_type>;

		struct SessionInfo
                {
                        TalkToClient::id_type id;
                        std::string channelName;
                };

		static void Run(size_t port, size_t threadNum);
		void RunService();

		bool HasRoom();
		void OnLoggedIn(TalkToClient::ptr client);

		io_service & get_service() { return service_; }
		ip::tcp::endpoint & get_endpoint() { return ep_; }
		ip::tcp::acceptor & get_acceptor() { return ac_; }

		size_t id() const { return id_; }
	private:
		static ptr New(unsigned short port) { return ptr{ new Channel{port} }; }

		void Start();
		void Init();
		void InitZookeeper();
		void InitRabbitMQ();
		void InitRedis();

		std::string GetIpAddress();		

		void DoAccept();
		void OnAccept(TalkToClient::ptr conn, const boost::system::error_code & ec);
	
		void AddClient(TalkToClient::ptr client);
		void FireClient(TalkToClient::id_type id);

		void PublishMessage(const std::string & body, const std::string & routingKey);	
		void SubscribeMQ();
		void HandleEnvelope(AmqpClient::Envelope::ptr_t envelope);
	
		void SaveSessionInfoToCache(TalkToClient::ptr client);
		void SaveSessionInfoToCache(TalkToClient::id_type id);
		bool LoadSessionInfo(TalkToClient::id_type id, SessionInfo &sessionInfo);
	private:
		io_service service_; //for separated select pool
		ip::tcp::endpoint ep_;
		ip::tcp::endpoint remote_ep_;
		ip::tcp::acceptor ac_;

		size_t id_;
		std::string name_;
		bool started_;
		container member_container_;
		boost::thread_group threads_;
		type_mutex mutex_start_;
		type_mutex mutex_member_;

		std::shared_ptr<ZkHandler> zk_;
		std::shared_ptr<MQHandler> mq_;
		std::string consumer_tag_;
		std::shared_ptr<redisContext> redis_;
	};
}
