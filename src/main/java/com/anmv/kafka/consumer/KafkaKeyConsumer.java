package com.anmv.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class KafkaKeyConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaKeyConsumer.class);

    /**
     * [Problem]
     * This function demo for consumer take more time to process one record,
     * this make LAG is increase from producer. We can avoid by using more
     * partition and more consumer read from them.
     *
     * @param record of Kafka when consumer pulled
     * @throws InterruptedException because of TimeUnit
     * @author anmv1
     */
    @KafkaListener(topics = "multiple-partition-topic")
    public void consume(ConsumerRecord<String, String> record) throws InterruptedException {
        LOG.info("Key: {}, Partition: {}, Message: {}", record.key(), record.partition(), record.value());
        TimeUnit.SECONDS.sleep(1);
    }

    /**
     * [Solved]
     * We solve above problem by add concurrency equal to number of partition's Topic
     *
     * @param record of Kafka when consumer pulled
     * @throws InterruptedException because of TimeUnit
     * @author anmv1
     */
//    @KafkaListener(topics = "multiple-partition-topic", concurrency = "3")
//    public void consume_fix(ConsumerRecord<String, String> record) throws InterruptedException {
//        LOG.info("Key: {}, Partition: {}, Message: {}", record.key(), record.partition(), record.value());
//        TimeUnit.SECONDS.sleep(1);
//    }
}
