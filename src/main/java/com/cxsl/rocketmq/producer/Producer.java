package com.cxsl.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

public class Producer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1. 创建DefaultMQProducer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("rocketmq-producer-group");
        //2. 设置Namesrv地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //3. 开启DefaultMQProducer
        defaultMQProducer.start();
        //4. 创建消息Message
        Message message = new Message("rocketmq-topic-01", "rocketmq-tag-01", "hello rocketmq".getBytes(RemotingHelper.DEFAULT_CHARSET));
        //5. 发送消息
        SendResult result = defaultMQProducer.send(message, 10000);
        System.out.println(result);
        //6. 关闭DefaultMQProducer
        defaultMQProducer.shutdown();
    }
}
