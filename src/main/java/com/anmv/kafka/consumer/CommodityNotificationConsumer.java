package com.anmv.kafka.consumer;

import com.anmv.kafka.entity.Commodity;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

//@Service
public class CommodityNotificationConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(CommodityNotificationConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "t-commodity", groupId = "cg-commodity-notification")
    public void consume(String message) throws JsonProcessingException, InterruptedException {
        Commodity commodity = objectMapper.readValue(message, Commodity.class);

        var randomDelayMilis = ThreadLocalRandom.current().nextInt(500, 2000);
        TimeUnit.MILLISECONDS.sleep(randomDelayMilis);

        LOG.info("Notification function: {}", commodity);
    }
}
