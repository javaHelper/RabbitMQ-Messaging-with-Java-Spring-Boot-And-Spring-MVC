package com.example;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class HeaderExchangeExample {
    public static final String HEADER_EXCHANGE = "Header-Exchange";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(HEADER_EXCHANGE, BuiltinExchangeType.HEADERS, true);


        //Create the Queues
        channel.queueDeclare("Mobile", true, false, false, null);
        channel.queueDeclare("AC", true, false, false, null);
        channel.queueDeclare("TV", true, false, false, null);

        // create bindings
        Map<String, Object> healthArgs = new HashMap<>();
        healthArgs.put("x-match", "any"); //Match any of the header
        healthArgs.put("item1", "mobile");
        healthArgs.put("item2", "mob");
        channel.queueBind("Mobile", HEADER_EXCHANGE, "", healthArgs);

        healthArgs = new HashMap<>();
        healthArgs.put("x-match", "any"); //Match any of the header
        healthArgs.put("item1", "tv");
        healthArgs.put("item2", "television");
        channel.queueBind("TV", HEADER_EXCHANGE, "*.tv.*");

        healthArgs = new HashMap<>();
        healthArgs.put("x-match", "all"); //Match any of the header
        healthArgs.put("item1", "mobile");
        healthArgs.put("item2", "ac");
        channel.queueBind("AC", HEADER_EXCHANGE, "#.ac");

        String message = "Message for Mobile and TV";

        Map<String, Object> map = new HashMap<>();
        map.put("item1","mobile");
        map.put("item2","television");

        AMQP.BasicProperties properties = new AMQP.BasicProperties();
        properties = properties.builder().headers(map).build();
        channel.basicPublish(HEADER_EXCHANGE, "", properties, message.getBytes());

        channel.close();
        connection.close();
    }
}