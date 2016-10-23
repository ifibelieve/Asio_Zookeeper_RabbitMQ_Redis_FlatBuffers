#include "MQHandler.h"

using namespace BaeServer;

MQHandler::MQHandler(const std::string & connectionString, 
			const std::string & exchangeName, 
			const std::string & exchangeType, 
			const std::string & queueName,
			const std::string & routingKey)
{
	Init(connectionString, exchangeName, exchangeType, queueName, routingKey);	
}

void MQHandler::Init(const std::string & connectionString, 
			const std::string & exchangeName, 
			const std::string & exchangeType, 
			const std::string & queueName, 
			const std::string & routingKey)
{
	conn_ = AmqpClient::Channel::Create(connectionString);
	conn_->DeclareExchange(exchangeName, exchangeType);
	queue_name_ = conn_->DeclareQueue(queueName);
	conn_->BindQueue(queueName, exchangeName, routingKey);
	
	exchange_name_ = exchangeName;
	consumer_tag_ = conn_->BasicConsume(queueName);
}

void MQHandler::PublishMessage(const std::string & body, const std::string & routingKey)
{
	auto message =  AmqpClient::BasicMessage::Create(body);			
	conn_->BasicPublish(exchange_name_, routingKey, message);
}

AmqpClient::Envelope::ptr_t MQHandler::ConsumeMessage()
{
	AmqpClient::Envelope::ptr_t envelope = conn_->BasicConsumeMessage(consumer_tag_);	
	return envelope;
}
