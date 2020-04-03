package com.rocketmq.demo.consumer.service.impl;

import com.rocketmq.demo.consumer.dao.OrderMapper;
import com.rocketmq.demo.consumer.dao.PointsMapper;
import com.rocketmq.demo.consumer.dto.Order;
import com.rocketmq.demo.consumer.dto.OrderDTO;
import com.rocketmq.demo.consumer.dto.Points;
import com.rocketmq.demo.consumer.service.PointsService;
import com.rocketmq.demo.consumer.util.SnowFlake;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.math.BigDecimal;

@Service
public class PointsServiceImpl implements PointsService {

    @Resource
    PointsMapper pointsMapper;

    Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void increasePoints(OrderDTO order) {
        Points points = new Points();
        points.setOrderNo(order.getOrderNo());
        //入库之前先查询，实现幂等
        if (pointsMapper.select(points).size()>0){
            logger.info("积分添加完成，订单已处理。{}",order.getOrderNo());
        }else{
            points.setId(SnowFlake.nextId());
            points.setUserId(order.getUserId());
            points.setOrderNo(order.getOrderNo());
            BigDecimal amount = order.getAmount();
            points.setPoints(amount.intValue()*10);
            points.setRemarks("商品消费共【"+order.getAmount()+"】元，获得积分"+points.getPoints());
            pointsMapper.insert(points);
            logger.info("已为订单号码{}增加积分。",points.getOrderNo());
        }
    }
}