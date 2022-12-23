package com.example;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class TopicExchangeExample {
    public static final String TOPIC_EXCHANGE = "Topic-Exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        String message = "Message for Mobile and TV";

        channel.exchangeDeclare(TOPIC_EXCHANGE, BuiltinExchangeType.TOPIC, true);

        //Create the Queues
        channel.queueDeclare("Mobile", true, false, false, null);
        channel.queueDeclare("AC", true, false, false, null);
        channel.queueDeclare("TV", true, false, false, null);

        // create bindings
        channel.queueBind("Mobile", TOPIC_EXCHANGE, "*.mobile.*");
        channel.queueBind("TV", TOPIC_EXCHANGE, "*.tv.*");
        channel.queueBind("AC", TOPIC_EXCHANGE, "#.ac");

        channel.basicPublish(TOPIC_EXCHANGE, "tv.mobile.ac", null, message.getBytes());

        channel.close();
        connection.close();
    }
}