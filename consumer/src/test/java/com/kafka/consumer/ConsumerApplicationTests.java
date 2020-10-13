package com.kafka.consumer;

//import com.kafka.consumer.deserializer.CustomerDeserializer;

import com.kafka.consumer.deserializer.CustomerDeserializer;
import com.kafka.consumer.dto.Customer;
import kafka.tools.ConsoleConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

@SpringBootTest
@Slf4j
class ConsumerApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void testConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "groupG G-1");

        KafkaConsumer<String, Customer> kafkaConsumer = new KafkaConsumer<String, Customer>(properties);
        kafkaConsumer.subscribe(Collections.singleton("my-first-topic"));
        while (true) {
            ConsumerRecords<String, Customer> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                log.info("Topic: " + record.topic());
                log.info("Offset: " + record.offset());
                log.info("Key: " + record.key());
                log.info("Value : " + record.value());
                log.info("Partition: " + record.partition());
            }

            kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    log.info("Inside commitAysnc Callback function ...");
                    if (e != null) {
                        e.printStackTrace(System.err);
                    }
                    System.out.println(map);
                }
            });
        }
    }

    @Test
    public void consumeStringData() {
        Properties properties = getConsumerProperties(StringDeserializer.class, StringDeserializer.class);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singleton("my-first-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                log.info("Topic: " + record.topic());
                log.info("Offset: " + record.offset());
                log.info("Key: " + record.key());
                log.info("Value : " + record.value());
                log.info("Partition: " + record.partition());
            }

            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    log.info("Inside commitAysnc Callback function ...");
                    if (e != null) {
                        e.printStackTrace(System.err);
                    }
                    System.out.println(map);
                }
            });
        }

    }

    private Properties getConsumerProperties(Class keyDeserializer, Class valueDeserializer) {
        final String bootstrap_server = "localhost:9092, localhost:9092";
        final String topic = "my-first-topic";
        final String group_id = "Group G1";

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_server);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group_id);
        return properties;
    }

}
