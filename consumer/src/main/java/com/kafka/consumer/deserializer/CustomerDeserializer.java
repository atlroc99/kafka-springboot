package com.kafka.consumer.deserializer;

import com.kafka.consumer.dto.Customer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class CustomerDeserializer extends JsonDeserializer<Customer> {
}
