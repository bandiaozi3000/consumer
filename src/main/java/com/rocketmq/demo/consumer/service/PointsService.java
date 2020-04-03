package com.rocketmq.demo.consumer.service;

import com.rocketmq.demo.consumer.dto.OrderDTO;

public interface PointsService {

     void increasePoints(OrderDTO order);
}
