package com.example;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class FanoutExchangeExample {
    public static final String FANOUT_EXCHANGE = "Fanout-Exchange";

    public static void declareExchange() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        channel.exchangeDeclare(FANOUT_EXCHANGE, BuiltinExchangeType.FANOUT, true);
        channel.close();
    }

    public static void declareQueues() throws IOException, TimeoutException {
        //Create a channel - do no't share the Channel instance
        Channel channel = ConnectionManager.getConnection().createChannel();
        //Create the Queues
        channel.queueDeclare("Mobile", true, false, false, null);
        channel.queueDeclare("AC", true, false, false, null);
        channel.queueDeclare("TV", true, false, false, null);
        channel.close();
    }

    public static void declareBindings() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        //Create bindings - (queue, exchange, routingKey) - routingKey != null
        channel.queueBind("Mobile", FANOUT_EXCHANGE, "");
        channel.queueBind("AC", FANOUT_EXCHANGE, "");
        //channel.queueBind("TV", "my-fanout-exchange", "");
        channel.close();
    }

    public static void subscribeMessage() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();

        channel.basicConsume("AC", true, ((consumerTag, message) -> {
            System.out.println(consumerTag);
            System.out.println("AC: " + new String(message.getBody()));
        }), System.out::println);
        channel.basicConsume("Mobile", true, ((consumerTag, message) -> {
            System.out.println(consumerTag);
            System.out.println("Mobile: " + new String(message.getBody()));
        }), System.out::println);
    }

    public static void publishMessage() throws IOException, TimeoutException {
        Channel channel = ConnectionManager.getConnection().createChannel();
        String message = "Main Power is ON";
        channel.basicPublish(FANOUT_EXCHANGE, "", null, message.getBytes());
        channel.close();
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        FanoutExchangeExample.declareExchange();
        FanoutExchangeExample.declareQueues();
        FanoutExchangeExample.declareBindings();

        //Threads created to publish-subscribe asynchronously
        Thread subscribe = new Thread() {
            @Override
            public void run() {
                try {
                    FanoutExchangeExample.subscribeMessage();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        };
        Thread publish = new Thread() {
            @Override
            public void run() {
                try {
                    FanoutExchangeExample.publishMessage();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }
        };
        subscribe.start();
        publish.start();
    }
}