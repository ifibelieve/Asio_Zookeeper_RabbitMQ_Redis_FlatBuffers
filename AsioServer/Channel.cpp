#include "Channel.h"
#include <mutex>
#include <vector>

using namespace BaeServer;
using namespace std;

Channel::~Channel()
{
	redisFree(redis_.get());
}

bool Channel::HasRoom()
{
	bool result = false;

	result = member_container_.size() < CHANNEL_CAPACITY;
	
	return result;
}

void Channel::OnLoggedIn(TalkToClient::ptr client)
{
	FireClient(client->id());
	AddClient(client);
}

void Channel::AddClient(TalkToClient::ptr client)
{
	type_lock lk{ mutex_member_ };
	if (0 == member_container_.count(client->id())) {
		member_container_.insert(
			std::pair<TalkToClient::id_type, TalkToClient::ptr>(client->id(), client));
	} else {
		member_container_[client->id()] = client;
	}
	lk.unlock();

	SaveSessionInfoToCache(client);
}

void Channel::FireClient(TalkToClient::id_type id)
{
	cerr << "\t[Channel] FireClient " << id << endl;

	SessionInfo sessionInfo;
	if (false == LoadSessionInfo(id, sessionInfo))
	{
		return;	
	} 
	
	string name = sessionInfo.channelName;
	
	if (name == name_)
	{
		auto it = member_container_.find(id);
		if (member_container_.end() != it)
		{
			it->second->Stop();
			member_container_.erase(it);
		}
	} 
	else 
	{
		ostringstream oss;
		oss << "FireClient." << id;
		PublishMessage(oss.str(), name);		
	}
}

void Channel::Run(size_t port, size_t thread_num)
{
	auto channel = Channel::New(port);
	channel->Start();	

	boost::thread_group threads;

	for (size_t i = 0; i < thread_num; i++)
	{
		boost::thread * thread = threads.create_thread(boost::bind(&Channel::RunService, channel));
	}
	
	threads.join_all();
}

void Channel::Start() 
{
	type_lock lk{ mutex_start_ };
        if (started_)
        {
                return;
        }
        started_ = true;
        lk.unlock();

        DoAccept();
	service_.post(boost::bind(&Channel::SubscribeMQ, shared_from_this()));
}

void Channel::RunService()
{
	service_.run();
}

void Channel::Init() 
{
	name_ = GetIpAddress() + ":" + std::to_string(ep_.port());
	InitZookeeper();
	InitRabbitMQ();
	InitRedis();
}

void Channel::InitZookeeper()
{
        zk_ = std::make_shared<ZkHandler>();
	zk_->CreateGroup("session");

	zk_->CreateMember(name_, "0");
}

void Channel::InitRabbitMQ()
{
	mq_ = std::make_shared<MQHandler>(
		"localHost",
		"xChannels",
		AmqpClient::Channel::EXCHANGE_TYPE_DIRECT,
		name_,
		name_
	);
}

void Channel::InitRedis()
{
	redis_ = std::shared_ptr<redisContext>(redisConnect("127.0.0.1", 6379));
	if (redis_->err) 
	{
		cerr << "\t[Channel] InitRedis error:" << redis_->errstr << endl;
	}
}

std::string Channel::GetIpAddress() {
	ip::tcp::resolver resolver(service_);
	ip::tcp::resolver::query query(ip::tcp::v4(), ip::host_name(), "");
	ip::tcp::resolver::iterator iter = resolver.resolve(query);
	ip::tcp::endpoint ep = *iter;

	return move(ep.address().to_string());
}

void Channel::DoAccept()
{
	std::cerr << "\t[Channel] do accept thread:" << std::this_thread::get_id() << std::endl;

	TalkToClient::ptr client { TalkToClient::New(service_) };

	std::cerr << "\t[Channel] channel:" << id_ << " port:" << ep_.port() << std::endl;

	ac_.async_accept(
		client->sock(),
		remote_ep_,
		MEM_FN3(OnAccept, client, _1));
}

void Channel::OnAccept(TalkToClient::ptr conn, const boost::system::error_code & ec)
{
	std::cerr << "\t[Channel] on accept ec:" << ec << " thread:" << std::this_thread::get_id() << std::endl;

	if (0 == ec)
	{
              	//weak_ptr
		std::weak_ptr<Channel> wp = shared_from_this();
		conn->set_channel(wp);
		//handle in service_'s threads
		service_.post(boost::bind(&TalkToClient::Start, conn));
	}

	DoAccept();
}

void Channel::PublishMessage(const std::string & body, const std::string  & routingKey)
{
	mq_->PublishMessage(body, routingKey);
}

void Channel::SubscribeMQ()
{
	cerr << "\t[Channel] SubscribeMQ" << endl;	

	AmqpClient::Envelope::ptr_t envelope =  mq_->ConsumeMessage();
	HandleEnvelope(envelope);
	service_.post(boost::bind(&Channel::SubscribeMQ, shared_from_this()));
}

void Channel::HandleEnvelope(AmqpClient::Envelope::ptr_t envelope)
{
	std::string body = envelope->Message()->Body();
	std::cerr << "\t[Channel] HandleEnvelope body:" << body << std::endl;	
	
	vector<string> strs;

	size_t begin = 0;
	size_t pos = string::npos;
	while (string::npos != (pos = body.find(".", begin))) 
	{
		strs.push_back(body.substr(begin, pos - begin));
		begin = pos + 1;
	}

	if (begin < body.size())
	{
		strs.push_back(body.substr(begin));
	}

	if ("FireClient" == strs[0])
	{
		TalkToClient::id_type id = std::stoull(strs[1]);
		FireClient(id);
	}
}

void Channel::SaveSessionInfoToCache(TalkToClient::ptr client)
{
	SaveSessionInfoToCache(client->id());
}

void Channel::SaveSessionInfoToCache(TalkToClient::id_type id)
{
	ostringstream oss;
	oss << "HMSET session:" << id << " channel " << name_.c_str();
	redisCommand(redis_.get(), oss.str().c_str());		
}

bool Channel::LoadSessionInfo(TalkToClient::id_type id, SessionInfo &sessionInfo)
{
	bool result = false;

	redisReply *reply = (redisReply *)redisCommand(redis_.get(), "HGETALL session:%zd", id);	
	
	if (NULL != reply && REDIS_REPLY_ARRAY ==  reply->type && 0 < reply->elements)	
	{
		sessionInfo.id = id;
		sessionInfo.channelName = reply->element[0]->integer;
		result = true;
	}

	return result;
}
