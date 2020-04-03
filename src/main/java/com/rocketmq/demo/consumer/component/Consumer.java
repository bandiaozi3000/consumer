package com.rocketmq.demo.consumer.component;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Component
public class Consumer {

    String consumerGroup = "consumer-group";
    DefaultMQPushConsumer consumer;

    @Resource
    OrderListener orderListener;
    
    @PostConstruct
    public void init() throws MQClientException {
        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr("127.0.0.1:9876");
        consumer.subscribe("order","*");
        consumer.registerMessageListener(orderListener);
        consumer.start();
    }
}