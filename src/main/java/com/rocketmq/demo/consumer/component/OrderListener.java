package com.rocketmq.demo.consumer.component;

import com.alibaba.fastjson.JSONObject;
import com.rocketmq.demo.consumer.dto.OrderDTO;
import com.rocketmq.demo.consumer.service.PointsService;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class OrderListener implements MessageListenerConcurrently {

    @Autowired
    PointsService pointsService;
    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext context) {
        logger.info("消费者线程监听到消息。");
        for (MessageExt message:list) {
            if (!processor(message)){
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
        }
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    /**
     * 消息处理，第3次处理失败后，发送邮件通知人工介入
     * @param message
     * @return
     */
    private boolean processor(MessageExt message){
        String body = new String(message.getBody());
        try {
            logger.info("消息处理....{}",body);
            int k = 1/0;
            return true;
        }catch (Exception e){
            if(message.getReconsumeTimes()>=3){
                logger.error("消息重试已达最大次数，将通知业务人员排查问题。{}",message.getMsgId());
                //发邮件
//                sendMail(message);
                return true;
            }
            return false;
        }
    }
}