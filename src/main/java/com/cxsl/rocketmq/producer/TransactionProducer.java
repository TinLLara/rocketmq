package com.cxsl.rocketmq.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.concurrent.*;

public class TransactionProducer {
    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        //1. 创建TransactionProducer
        TransactionMQProducer transactionProducer = new TransactionMQProducer("rocketmq-producer-group");
        //2. 设置Namesrv地址
        transactionProducer.setNamesrvAddr("127.0.0.1:9876");
        //创建事务监听器，并分配给生产者，用于执行本地事务与事务检查
        TransactionListener transactionListener = new TransactionListenerImpl();
        transactionProducer.setTransactionListener(transactionListener);
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 200, TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(1000), new ThreadFactory(){
                @Override
                public Thread newThread(Runnable r) {
                    Thread thread = new Thread(r);
                    thread.setName("rocketmq-transaction-msg-check-thread");
                    return thread;
                }
            }
        );
        transactionProducer.setExecutorService(executorService);
        //3. 开启transactionProducer
        transactionProducer.start();
        //4. 创建消息Message
        Message message = new Message("rocketmq-transaction-topic-01", "rocketmq-transaction-tag-01", "hello transaction rocketmq-02".getBytes(RemotingHelper.DEFAULT_CHARSET));
        //5. 发送消息
        //进行事务类型消息发送
        TransactionSendResult result = transactionProducer.sendMessageInTransaction(message, "rocketmq-transaction");
        System.out.println(result);
        //6. 关闭transactionProducer
        transactionProducer.shutdown();
    }
}
