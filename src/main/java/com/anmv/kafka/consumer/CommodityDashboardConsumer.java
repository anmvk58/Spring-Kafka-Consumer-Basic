package com.anmv.kafka.consumer;

import com.anmv.kafka.entity.Commodity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

//@Service
public class CommodityDashboardConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CommodityDashboardConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "t-commodity", groupId = "cg-commodity-dashboard")
    public void consumer(String message) throws JsonProcessingException {
        Commodity commodity = objectMapper.readValue(message, Commodity.class);
        LOG.info("Dashboard function: {}", commodity);
    }
}
