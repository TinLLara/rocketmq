package com.cxsl.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class OrderProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1. 创建DefaultMQProducer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("rocketmq-producer-group");
        //2. 设置Namesrv地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //3. 开启DefaultMQProducer
        defaultMQProducer.start();
        //4. 创建消息Message
        Message message = new Message("rocketmq-order-topic-01", "rocketmq-order-tag-01", "hello order rocketmq-02".getBytes(RemotingHelper.DEFAULT_CHARSET));
        //5. 发送消息
        //指定队列或分区来进行消息投递，消息队列选择器，第三个参数为所选队列下标，从0开始；
        SendResult result = defaultMQProducer.send(message, new MessageQueueSelector(){
            @Override
            public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                //获取队列的下标-
                Integer index = (Integer)o;
                //返回选取的队列
                return list.get(index);
            }
        }, 0);
        System.out.println(result);
        //6. 关闭DefaultMQProducer
        defaultMQProducer.shutdown();
    }
}
