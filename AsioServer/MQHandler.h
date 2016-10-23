#pragma once
#include <iostream>
#include <SimpleAmqpClient/SimpleAmqpClient.h>

namespace BaeServer
{
	class MQHandler
	{
	public:
		MQHandler(const std::string & connectionString, 
			  const std::string & exchangeName, 
			  const std::string & exchangeType, 
			  const std::string & queueName,
			  const std::string & routingKey);

		void PublishMessage(const std::string & body, const std::string & routingKey = "");
		AmqpClient::Envelope::ptr_t ConsumeMessage();
	private:
		void Init(const std::string & connectionString, 
			  const std::string & exchangeName, 
			  const std::string & exchangeType, 
			  const std::string & queueName,
			  const std::string & routingKey);

		AmqpClient::Channel::ptr_t conn_;
		std::string exchange_name_;
		std::string queue_name_;
		std::string consumer_tag_;
	};
};
