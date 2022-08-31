package com.x.topic;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 主题模式
 * 消费者
 *
 * "*.*.rabbit"：最后一个单词是 rabbit 的 3 个单词，
 * "lazy.#"：第一个单词是 lazy 的多个单词，如生产者指定绑定键为"lazy.brown.fox"则路由到 ReceiveLogsTopic02
 *
 */
public class ReceiveLogsTopic02 {
    //交换机的名称
    public static final String EXCHANGE_NAME = "topic_logs";

    //接收消息
    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMQUtils.getChannel();
        //声明交换机，使用枚举
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
        //声明队列
        String queueName = "Q2";
        channel.queueDeclare(queueName,false,false,false,null);
        //交换机和队列绑定
        channel.queueBind(queueName,EXCHANGE_NAME,"*.*.rabbit");
        channel.queueBind(queueName,EXCHANGE_NAME,"lazy.#");

        System.out.println("等待接收消息...");

        DeliverCallback deliverCallback = (consumerTag, message) -> {
            System.out.println(new String(message.getBody(),"UTF-8"));
            System.out.println("接收队列："+queueName+"  绑定键:"+message.getEnvelope().getRoutingKey());
        };
        //接收消息
        channel.basicConsume(queueName,true,deliverCallback,consumerTag ->{});
    }
}
