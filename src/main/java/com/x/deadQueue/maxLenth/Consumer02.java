package com.x.deadQueue.maxLenth;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**死信队列
 * 死信消费者
 * 接收因为正常队列无法消费的信息
 */
public class Consumer02 {
    //死信队列的名称
    public static final String DEAD_QUEUE = "dead_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        Channel channel = RabbitMQUtils.getChannel();

        System.out.println("等待接收死信消息...");

        DeliverCallback deliverCallback = (consumerTag, message) ->{
            System.out.println("Consumer02接受的消息是："+new String(message.getBody(),"UTF-8"));
        };

        channel.basicConsume(DEAD_QUEUE,true,deliverCallback,consumerTag -> {});
    }
}
