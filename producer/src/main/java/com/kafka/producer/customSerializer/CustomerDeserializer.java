package com.kafka.producer.customSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.dto.Customer;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class CustomerDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public Customer deserialize(String s, byte[] bytes) {
        Customer customer = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            customer = objectMapper.readValue(bytes, Customer.class);
        }catch (Exception e) {
            e.printStackTrace(System.err);
        }
        return customer;
    }

    @Override
    public Customer deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    @Override
    public void close() {

    }
}
