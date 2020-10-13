package com.kafka.producer.configuration;

import com.kafka.producer.dto.Customer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class ProducerConfiguration {

    private final KafkaAdminConfig kafkaAdminConfig;

    public ProducerConfiguration(KafkaAdminConfig kafkaAdminConfig) {
        this.kafkaAdminConfig = kafkaAdminConfig;
    }

    @Bean
    public ProducerFactory<String, Object> getProducerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAdminConfig.getBootstrapServer());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaAdminConfig.getKeySerializer());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaAdminConfig.getCustomValueSerializer());
        DefaultKafkaProducerFactory<String, Object> producerFactory = new DefaultKafkaProducerFactory<>(configs);
        return producerFactory;
    }

    @Bean
    public KafkaTemplate getKafkaTemaplate() {
        return new KafkaTemplate(getProducerFactory());
    }
}
