package com.example;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class DirectExchangeExample {

    public static final String DIRECT_EXCHANGE = "Direct-Exchange";

    public static void declareExchange() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        channel.exchangeDeclare(DIRECT_EXCHANGE, BuiltinExchangeType.DIRECT, true);
        channel.close();
    }

    public static void declareQueues() throws IOException, TimeoutException {
        //Create a channel - don't share the Channel instance
        Channel channel = ConnectionManager.getConnection().createChannel();
        //queueDeclare  - (queueName, durable, exclusive, autoDelete, arguments)
        channel.queueDeclare("Mobile", true, false, false, null);
        channel.queueDeclare("AC", true, false, false, null);
        channel.queueDeclare("TV", true, false, false, null);
        channel.close();
    }

    public static void declareBindings() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        //Create bindings - (queue, exchange, routingKey)
        channel.queueBind("Mobile", DIRECT_EXCHANGE, "mobile");
        channel.queueBind("AC", DIRECT_EXCHANGE, "ac");
        channel.queueBind("TV", DIRECT_EXCHANGE, "tv");
        channel.close();
    }

    public static void subscribeMessage() throws IOException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        channel.basicConsume("Mobile", true, ((consumerTag, message) -> {
            System.out.println(consumerTag);
            System.out.println("Mobile:" + new String(message.getBody()));
        }), System.out::println);
        channel.basicConsume("AC", true, ((consumerTag, message) -> {
            System.out.println(consumerTag);
            System.out.println("AC:" + new String(message.getBody()));
        }), System.out::println);
        channel.basicConsume("TV", true, ((consumerTag, message) -> {
            System.out.println(consumerTag);
            System.out.println("TV:" + new String(message.getBody()));
        }), System.out::println);
    }


    //Publish the message
    public static void publishMessage() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        String message = "Direct message - Turn on the Home Appliances ";
        channel.basicPublish(DIRECT_EXCHANGE, "mobile", null, message.getBytes());
        channel.close();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        DirectExchangeExample.declareExchange();
        DirectExchangeExample.declareQueues();
        DirectExchangeExample.declareBindings();

        Thread subscribe = new Thread() {
            @Override
            public void run() {
                try {
                    DirectExchangeExample.subscribeMessage();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread publish = new Thread() {
            @Override
            public void run() {
                try {
                    DirectExchangeExample.publishMessage();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        };
        subscribe.start();
        publish.start();
    }
}