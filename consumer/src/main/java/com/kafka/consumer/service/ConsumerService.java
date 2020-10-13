package com.kafka.consumer.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
@Service
public class ConsumerService {

    public void testConsume() {
        Properties consumerProps = new Properties();
        consumerProps.setProperty("bootstrap.servers", "localhost:9092");
        consumerProps.setProperty("group.id", "group_string");
        consumerProps.setProperty("enable.auto.commit", "true");
        consumerProps.setProperty("auto.commit.interval.ms", "1000");
        consumerProps.setProperty("key.deserializer", StringDeserializer.class.getName());
        consumerProps.setProperty("auto.offset.reset", "earliest");
        consumerProps.setProperty("value.deserializer", StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList("my-first-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            log.info(">>> Printing INFO");
            log.info("Record count: " + records.count());
            log.info("Record isEmpty: " + records.isEmpty());

            for (ConsumerRecord record : records) {
                log.info("PRINT");
                log.info("--------------------------------------");
                log.info("Offset: " + record.offset());
                log.info("Partition: " + record.partition());
                log.info("Topic: " + record.topic());
                log.info(">>>> Value / message: " + record.value());
                log.info("--------------------------------------");
            }
            log.info(">>>Finish Printing INFO");
        }

    }
}
