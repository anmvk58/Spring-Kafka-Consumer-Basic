package com.anmv.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class BalanceConsumerDemo {

    private static final Logger LOG = LoggerFactory.getLogger(BalanceConsumerDemo.class);

    @KafkaListener(topics = "t-rebalance", concurrency = "3")
    public void balanceConsumer(ConsumerRecord<String, String> record) {
        LOG.info("Partition: {}  -  Offset: {}  -  Message: {}", record.partition(), record.offset(), record.value());
    }
}
