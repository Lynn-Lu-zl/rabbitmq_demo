package com.x.deadQueue.ttl;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DeliverCallback;
import com.x.utils.RabbitMQUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**死信队列
 * 正常队列消费者
 * 启动之后关闭该消费者 模拟其接收不到消息
 *
 * 成为死信后转发到死信队列
 */
public class Consumer01 {
    //普通交换机的名称
    public static final String NORMAL_EXCHANGE = "normal_exchange";
    //死信交换机的名称
    public static final String DEAD_EXCHANGE = "dead_exchange";

    //普通队列的名称
    public static final String NORMAL_QUEUE = "normal_queue";
    //死信队列的名称
    public static final String DEAD_QUEUE = "dead_queue";

    public static void main(String[] args) throws IOException, TimeoutException {

        Channel channel = RabbitMQUtils.getChannel();

        //声明死信和普通交换机，类型为direct
        channel.exchangeDeclare(NORMAL_EXCHANGE, BuiltinExchangeType.DIRECT);
        channel.exchangeDeclare(DEAD_EXCHANGE, BuiltinExchangeType.DIRECT);

        //声明普通队列
        Map<String,Object> arguments = new HashMap<>();
        //过期时间 10s 由生产者指定 更加灵活
        //arguments.put("x-message-ttl",10000);
        //正常的队列设置死信交换机，图中红箭头，一旦正常队列不能消费消息就转发到死信队列
        arguments.put("x-dead-letter-exchange",DEAD_EXCHANGE);
        //设置死信routingKey，不然正常队列不知道要路由消息到哪个队列
        arguments.put("x-dead-letter-routing-key","lisi");

        //声明正常的队列，最后一个参数arguments完成了正常队列转发到死信队列的条件和过程
        channel.queueDeclare(NORMAL_QUEUE,false,false,false,arguments);
        /////////////////////////////////////////////////////////////////////////
        //声明死信队列
        channel.queueDeclare(DEAD_QUEUE,false,false,false,null);

        //绑定普通的交换机与队列
        channel.queueBind(NORMAL_QUEUE,NORMAL_EXCHANGE,"zhangsan");

        //绑定死信的交换机与死信的队列
        channel.queueBind(DEAD_QUEUE,DEAD_EXCHANGE,"lisi");
        System.out.println("等待接收消息...");

        DeliverCallback deliverCallback = (consumerTag, message) ->{
            System.out.println("Consumer01接受的消息是："+new String(message.getBody(),"UTF-8"));
        };

        channel.basicConsume(NORMAL_QUEUE,true,deliverCallback,consumerTag -> {});
    }

}
