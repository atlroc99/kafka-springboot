package com.kafka.producer.customSerializer;

import com.kafka.producer.dto.Customer;
import org.springframework.kafka.support.serializer.JsonSerializer;

public class CustomerSerializer extends JsonSerializer<Customer> {
}
