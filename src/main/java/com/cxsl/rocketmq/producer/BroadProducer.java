package com.cxsl.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class BroadProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1. 创建DefaultMQProducer
        DefaultMQProducer defaultMQProducer = new DefaultMQProducer("rocketmq-broad-producer-group");
        //2. 设置Namesrv地址
        defaultMQProducer.setNamesrvAddr("127.0.0.1:9876");
        //3. 开启DefaultMQProducer
        defaultMQProducer.start();
        //4. 创建消息Message
        List<Message> messages = new ArrayList<Message>();
        for (int i = 0; i < 10; i ++) {
            Message message = new Message("rocketmq-topic-01", "rocketmq-tag-01", ("hello rocketmq " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            messages.add(message);
        }
        //5. 发送消息
        SendResult result = defaultMQProducer.send(messages, 10000);
        System.out.println(result);
        //6. 关闭DefaultMQProducer
        defaultMQProducer.shutdown();
    }
}
