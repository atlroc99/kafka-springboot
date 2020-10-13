package com.kafka.consumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class SpringKafkaConsumer {

    Logger logger = LoggerFactory.getLogger(SpringKafkaConsumer.class);

    @KafkaListener(id = "ID:1", topics = "my-second-topic")
    public void consumeMessage(String payload) throws JsonProcessingException {
        logger.info(">>>> ConsumeMessage method has been invoked!!!");
        Customer customer = new ObjectMapper().readValue(payload, Customer.class);
        if (customer != null) {
            logger.info("ID: " + customer.getID());
            logger.info("DEPT: " + customer.getDept());
            logger.info("NAME: " + customer.getName());
            logger.info("SALARY: " + customer.getSalary());
            customer.getAddressList().forEach(System.out::println);
        }
    }
}

