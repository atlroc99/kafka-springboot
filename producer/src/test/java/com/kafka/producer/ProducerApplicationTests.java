package com.kafka.producer;

import com.kafka.producer.configuration.KafkaAdminConfig;
import com.kafka.producer.customSerializer.CustomerSerializer;
import com.kafka.producer.dto.Address;
import com.kafka.producer.dto.Customer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@SpringBootTest
@Slf4j
class ProducerApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    public void testSendMessage() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9092");
        properties.setProperty("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
        properties.setProperty("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer(properties);
        final String TOPIC = "my-first-topic";
        final String key = "Retry sync";
        final String value = "retying sync send method...";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);
        Future<RecordMetadata> future = kafkaProducer.send(producerRecord);
        Date date = new Date();
        printMetaData(future.get());
    }

    private void printMetaData(RecordMetadata recordMetadata) {
        System.out.println("\n------------- PRINT RECORD METADATA -------------");
        System.out.println("topic: " + recordMetadata.topic());
        System.out.println("offset: " + recordMetadata.offset());
        System.out.println("partition: " + recordMetadata.partition());
        System.out.println("serializedKeySize: " + recordMetadata.serializedKeySize());
        System.out.println("serializedValueSize: " + recordMetadata.serializedValueSize());
        System.out.println(getFormattedDateTime(recordMetadata.timestamp()));
        System.out.println("\n------------- PRINT RECORD ENDS -------------");
    }

    private String getFormattedDateTime(long timestamp) {
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy");
        TimeZone timeZone = TimeZone.getDefault();
        timeZone.setID("EST");
        sdf.setTimeZone(timeZone);

        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTimeInMillis(timestamp);
        final int HOUR = calendar.get(Calendar.HOUR_OF_DAY);
        final int MINUTE = calendar.get(Calendar.MINUTE);
        final int SECOND = calendar.get(Calendar.SECOND);
        final int MILLISECOND = calendar.get(Calendar.MILLISECOND);
        return sdf.format(new Date(timestamp)) + " at: " + HOUR + ":" + MINUTE + ":" + SECOND + ":" + MILLISECOND;
    }



    @Test
    public void testProducerWithCallback() throws ExecutionException, InterruptedException {

        final String TOPIC = "my-first-topic";
        final String key = "async-message";
        final String value = ">>>>> Async message 111 localhost:902 ><<<<<<< ";
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, key, value);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092, localhost:9092");
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        Integer NUMBER_RETRIES = 10;
        props.setProperty("retries", NUMBER_RETRIES.toString());

        KafkaProducer<String, String> kafkaProducer= new KafkaProducer<>(props);
        int i = 0;

        Future<RecordMetadata> metaData = kafkaProducer.send(producerRecord, new ProducerCallback());
    }


    private class ProducerCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            System.out.println(">>> ProducerCallback onCompletion has been invoked...");
            if (e != null) {
                e.printStackTrace(System.err);
            }
            printMetaData(recordMetadata);
        }
    }

    @Test
    public void testObjectTypeSerializer() throws ExecutionException, InterruptedException {

        Address address = Address.builder()
                .street("123 Street")
                .city("L'ville")
                .state("GA")
                .build();

        Customer customer = new Customer();
        customer.setID(UUID.randomUUID().toString());
        customer.setName("Jason");
        customer.setDept("Movie");
        customer.setSalary(12000);
        List<Address> addresses = new ArrayList<>();
        addresses.add(Address.builder().street("123 Street").city("Atlanta").state("GA").build());
        addresses.add(Address.builder().street("456 Street").city("Silver Spring").state("MD").build());
        addresses.add(Address.builder().street("789 Street").city("New York City").state("NY").build());
        customer.setAddressList(addresses);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class);

        Producer<String,Customer> kafkaProducer = new KafkaProducer<>(properties);
        Producer<String,Customer> kafkaProducer2 = new KafkaProducer<>(properties);

        final String TOPIC = "my-first-topic";
        final String key = "producer 1 : customer-info";

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<>(TOPIC, key, customer);
        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                log.info(">>> Inside the call back function");
                if (e != null) {
                    e.printStackTrace(System.err);
                }
                printMetaData(recordMetadata);
            }
        });

        //flushes data
        kafkaProducer.flush();

        //flushes and close producer
        kafkaProducer.close();
    }

    @Autowired
    KafkaAdminConfig topicConfig;

    @Test
    public void testKafkaAdminConfig() {
        KafkaAdmin kafkaAdmin = topicConfig.getKafkaAdmin();
        Map<String, Object> configurationProperties = kafkaAdmin.getConfigurationProperties();
        Assertions.assertTrue(!configurationProperties.isEmpty());
        NewTopic topic = topicConfig.getNewTopic();
        Assertions.assertNotNull(topic);
        log.info("Topic.name : "  + topic.name());
        log.info("Topic.replicationFactor : "  + topic.replicationFactor());
        log.info("Topic.numPartitions: "  + topic.numPartitions());
    }
}
