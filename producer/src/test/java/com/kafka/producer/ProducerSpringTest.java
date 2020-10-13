package com.kafka.producer;

import com.kafka.producer.configuration.KafkaAdminConfig;
import com.kafka.producer.dto.Address;
import com.kafka.producer.dto.Customer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@SpringBootTest
public class ProducerSpringTest {

    Logger logger = LoggerFactory.getLogger(ProducerSpringTest.class);

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Autowired
    KafkaAdminConfig kafkaAdminConfig;

    @Test
    public void testSendMessage() {
        String topic = kafkaAdminConfig.getTopic();
        Assertions.assertEquals("my-second-topic", topic);
        Customer customer = Customer.builder()
                .ID(UUID.randomUUID().toString())
                .name("Mohammad Zaman")
                .dept("Developer")
                .salary(11000)
                .addressList(Arrays.asList(
                        Address.builder().street("9th Avenue").city("Silver Spring").state("Maryland").build(),
                        Address.builder().street("Manhattan Avenue").city("New York City").state("New York").build()))
                .build();

        kafkaTemplate.send(topic, customer);
    }


    @Test
    public void testSendString() throws ExecutionException, InterruptedException {
        String msg = "Testing to send a string message where Value Serializer is not a String";
        ListenableFuture future = kafkaTemplate.send(kafkaAdminConfig.getTopic(), msg);

        future.addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onSuccess(SendResult<String, Object> stringObjectSendResult) {
                ProducerRecord<String, Object> producerRecord = stringObjectSendResult.getProducerRecord();
                logger.debug("producer record: " + producerRecord);
                RecordMetadata recordMetadata = stringObjectSendResult.getRecordMetadata();

                logger.debug("Timestamp : " + recordMetadata.timestamp() + "\n" +
                        "Topic: " + recordMetadata.topic() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Position/ offset: " + recordMetadata.offset() + "\n"
                );

            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.info("Error occurred while trying to send the msg: " + msg);
            }
        });

        kafkaTemplate.flush();
    }

    @Test
    public void testSendCustomer() {
        kafkaTemplate.send(kafkaAdminConfig.getTopic(), Customer.builder()
                .ID(UUID.randomUUID().toString())
                .name("Mohammad Como")
                .dept("Research & Development")
                .salary(10000)
                .addressList(Arrays.asList(Address.builder().street("123 Parkway").state("MD").city("Silver Spring").build(),
                        Address.builder().street("432 JonDoe Avenue").state("CA").city("San Diego").build()))
                .build()
        ).addCallback(new ListenableFutureCallback<SendResult<String, Object>>() {
            @Override
            public void onSuccess(SendResult<String, Object> stringObjectSendResult) {
                logger.info("Customer add SUCCESSFUL at OFFSET: "
                        + stringObjectSendResult.getRecordMetadata().offset()
                        + " | PARTITION: " + stringObjectSendResult.getRecordMetadata().partition());

                Customer customer = (Customer) stringObjectSendResult.getProducerRecord().value();
                logger.info("ID: " + customer.getID());
                logger.info("Name: " + customer.getName());
                logger.info("Department: " + customer.getDept());
                logger.info("Salary: " + customer.getSalary());

                customer.getAddressList().forEach(System.out::println);
            }

            @Override
            public void onFailure(Throwable throwable) {
                logger.info("Error Sending Customer message: " +  throwable.getMessage());
            }
        });
    }


    //Customer add SUCCESSFUL at OFFSET: 3 | PARTITION: 0
    //Customer add SUCCESSFUL at OFFSET: 4 | PARTITION: 0

}
