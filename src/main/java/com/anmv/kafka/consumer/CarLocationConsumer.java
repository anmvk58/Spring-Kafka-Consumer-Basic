package com.anmv.kafka.consumer;

import com.anmv.kafka.entity.CarLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class CarLocationConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(CarLocationConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "t-car-location", groupId = "cg-all-location")
    public void listenAll(String message) throws JsonProcessingException {
        var carLocation = objectMapper.readValue(message, CarLocation.class);
        LOG.info("Listen All : {}", carLocation);
    }

    @KafkaListener(topics = "t-car-location", groupId = "cg-far-location", containerFactory = "farLocationContainerFactory")
    public void listenFar(String message) throws JsonProcessingException {
        var carLocation = objectMapper.readValue(message, CarLocation.class);

//        this method is stupid and no leverage Spring :D
       /* if (carLocation.getDistance() < 100) {
            return;
        }*/

        LOG.info("Listen Far : {}", carLocation);
    }
}
