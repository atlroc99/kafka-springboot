package com.kafka.producer.configuration;

import lombok.Data;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
public class KafkaAdminConfig {

    @Value("${kafka.bootstrapAddress}")
    private String bootstrapServer;

    @Value("${kafka.topic}")
    private String topic;

    @Value("${kafka.numPartitions}")
    private int numPartitions;

    @Value("${kafka.replicationFactor}")
    private int replicationFactor;

    @Value("${kafka.key-serializer}")
    private String keySerializer;

    @Value("${kafka.value-customerSerializer}")
    private String customValueSerializer;

    @Bean
    public KafkaAdmin getKafkaAdmin() {
        Map<String, Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        return new KafkaAdmin(config);
    }

    @Bean
    public NewTopic getNewTopic(){
        return new NewTopic(topic, numPartitions, (short) replicationFactor);
    }

}
