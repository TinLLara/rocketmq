package com.cxsl.rocketmq.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class OrderConsumer {
    public static void main(String[] args) throws MQClientException {
        //1. 创建DefaultMQPushConsumer
        DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("rocketmq-consumer-group");
        //2. 设置Namesrv地址
        defaultMQPushConsumer.setNamesrvAddr("127.0.0.1:9876");
        //设置消息拉取的最大数
        defaultMQPushConsumer.setConsumeMessageBatchMaxSize(5);
        //3. 设置subscribe，订阅需要读取消息的主题信息 "*"-所有tag的消息 多个tag可以使用 "aaa||bbb||ccc"
        defaultMQPushConsumer.subscribe("rocketmq-order-topic-01", "*");
        //4. 创建消息监听MessageListener
        defaultMQPushConsumer.setMessageListener(new MessageListenerOrderly() {

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                //遍历消息
                for (MessageExt msg : list) {
                    try {
                        //5. 获取消息信息
                        //获取消息的相关信息
                        String topic = msg.getTopic();
                        String tag = msg.getTags();
                        String result = new String(msg.getBody(), RemotingHelper.DEFAULT_CHARSET);
                        System.out.println("消费者消费消息：topic---" + topic + " tag---" + tag + " result---" + result);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                        //如果异常，重试
                        return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                    }
                }
                //6. 返回消息读取的状态
                //返回消费成功，对应ack
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        //启动消费者
       defaultMQPushConsumer.start();
       //使用@PreDestroy unsubscribe释放订阅
    }
}
