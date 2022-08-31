package com.x.delayQueue.consumer;

import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Date;

/**
 * 消费者
 */
@Slf4j
@Component
public class DeadLetterQueueConsumer {
    //消费者接收死信队列QB的消息
    @RabbitListener(queues = "QD")
    public void receiveD(Message message, Channel channel) throws IOException {
        //获取死信队列的消息体转成字符串
        String msg = new String(message.getBody());
        log.info("当前时间：{},收到死信队列信息{}", new Date().toString(), msg);
    }
}
