package com.example.consumer.service;

import com.example.consumer.dto.Customer;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaListenerService {

    private final CustomerService customerService;

    public KafkaListenerService(CustomerService customerService) {
        this.customerService = customerService;
    }

    @KafkaListener(id = "ID:1", topics = "my-second-topic")
    public void consumeMessage(String payload) throws JsonProcessingException {
        System.out.println("INVOKED Consumer messge >>>> ");
        System.out.println(payload);
        ObjectMapper objectMapper = new ObjectMapper();
        Customer customer = objectMapper.readValue(payload, Customer.class);
        customerService.saveCustomer(customer);
    }
}