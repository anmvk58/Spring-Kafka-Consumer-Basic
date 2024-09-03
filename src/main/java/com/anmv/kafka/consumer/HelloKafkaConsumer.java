package com.anmv.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class HelloKafkaConsumer {

    @KafkaListener(topics = "hello_kafka")
    public void consume(ConsumerRecord<?, ?> record) {
        System.out.println(record.value());
    }
}
