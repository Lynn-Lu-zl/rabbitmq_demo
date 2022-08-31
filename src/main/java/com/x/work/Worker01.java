package com.x.work;

import com.rabbitmq.client.CancelCallback;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 工作线程
 * 相当于之前的消费者
 */
public class Worker01 {
    //队列的名称
    public static final String QUEUE_NAME="hello";


    //接收消息
    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMQUtils.getChannel();

        //消息的接受
        DeliverCallback deliverCallback = (consumerTag, message) -> System.out.println("接收到的消息:"+new String(message.getBody()));

        //消息接受被取消时，执行下面的内容
        CancelCallback cancelCallback = consumerTag -> {
            System.out.println(consumerTag+"消息被消费者取消消费接口回调逻辑");
        };

        //消息的接受
        System.out.println("工作线程3等待接收消息.......");
        channel.basicConsume(QUEUE_NAME,true,deliverCallback,cancelCallback);
    }
}
