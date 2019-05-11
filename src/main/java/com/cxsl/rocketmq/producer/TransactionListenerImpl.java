package com.cxsl.rocketmq.producer;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.ConcurrentHashMap;

public class TransactionListenerImpl implements TransactionListener {
    //用于存储本地事务执行状态，key为事务id（来自borker的事务），value为事务状态
    private ConcurrentHashMap<String, Integer> localTranStatus = new ConcurrentHashMap<>();

    //执行本地事务，prepare消息投递到broker的halftopic（消费者不可见）成功后，由broker进行回调执行本地事务
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        String transactionId = message.getTransactionId();
        //0:事务执行中或状态未知，1:事务执行成功，2:事务执行失败
        localTranStatus.put(transactionId, 0);
        System.out.println("broker接收prepare消息成功，回调执行本地事务开始----");
        try {
            Thread.sleep(1000 * 120 + 10000);
            System.out.println("broker接收prepare消息成功，回调执行本地事务成功----");
            localTranStatus.put(transactionId, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println("broker接收prepare消息成功，回调执行本地事务失败----");
            localTranStatus.put(transactionId, 2);
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        //执行成功，提供broker的分布式事务
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    //消息回查，检查本地事务
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        //获取对应事务的id
        String msgId = messageExt.getMsgId();
        //获取对应事务的状态，并返回给broker
        Integer status = localTranStatus.get(msgId);
        System.out.println("消息回查开始>>>>>transactonid：" + msgId + " 状态为：" + status);
        switch (status) {
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}
