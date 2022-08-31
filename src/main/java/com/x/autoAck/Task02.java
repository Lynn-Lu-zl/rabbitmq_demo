package com.x.autoAck;

import com.rabbitmq.client.Channel;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**
 * 手动应答案例
 * 消费在手动应答时是不丢失，放在队列中重新消费
 * 消息生产者
 */
public class Task02 {
    //队列名称
    public static final String TASK_QUEUE_NAME = "ACK_QUEUE";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMQUtils.getChannel();

        //声明队列
        channel.queueDeclare(TASK_QUEUE_NAME,true,false,false,null);
        //在控制台中输入信息
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入信息：");
        while (scanner.hasNext()){
            String message = scanner.next();
            channel.basicPublish("",TASK_QUEUE_NAME,null,message.getBytes("UTF-8"));
            System.out.println("生产者发出消息:"+ message);
        }

    }
}
