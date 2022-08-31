package com.x.direct;

import com.rabbitmq.client.Channel;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

/**路由模式
 * 生产者
 * 发送消息但是有绑定键routingKey
 * 当生产者生产消息到 direct_logs 交换机里，该交换机会检测消息的 routingKey 条件，然后分配到满足条件的队列里，最后由消费者从队列消费消息
 */
public class DirectLogs {
    //交换机名称
    public static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws IOException, TimeoutException {
        Channel channel = RabbitMQUtils.getChannel();


        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()){
            String message = scanner.next();
            //发布消息指定了routingKey 为info，只有ReceiveLogsDirect01能收到消息
            channel.basicPublish(EXCHANGE_NAME,"info",null,message.getBytes("UTF-8"));
            //指定了routingKey 为error，只有ReceiveLogsDirect02能收到消息
            //channel.basicPublish(EXCHANGE_NAME,"error",null,message.getBytes("UTF-8"));
            System.out.println("生产者发出消息:"+message);
        }
    }
}
