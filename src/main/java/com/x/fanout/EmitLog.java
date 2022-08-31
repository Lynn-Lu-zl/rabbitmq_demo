package com.x.fanout;

import com.rabbitmq.client.Channel;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 广播交换机
 * 生产者：进行发消息给交换机
 * 发送一条消息，两个消费者都能收到
 */
public class EmitLog {
    //交换机名称
    public static final String EXCHANGE_NAME = "logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMQUtils.getChannel();
        /**
         * 声明一个交换机exchange
         * 1.exchange的名称
         * 2.exchange的类型
         */
        channel.exchangeDeclare(EXCHANGE_NAME,"fanout");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()){
            String message = scanner.next();
            channel.basicPublish(EXCHANGE_NAME,"",null,message.getBytes("UTF-8"));
            System.out.println("生产者发出消息:"+message);
        }
    }
}
