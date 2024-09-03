package com.anmv.kafka.consumer;

import com.anmv.kafka.entity.Employee;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class EmployeeJsonConsumer {

    private final static Logger LOG = LoggerFactory.getLogger(EmployeeJsonConsumer.class);

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "json-employee")
    public void employeeJsonComsumer(String message) throws JsonProcessingException {
        Employee employee = objectMapper.readValue(message, Employee.class);
        LOG.info(employee.toString());
    }

}
