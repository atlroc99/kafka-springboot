package com.kafka.producer.controller;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequestMapping("/producerApi")
public class ProducerController {

    final Logger logger = LoggerFactory.getLogger(ProducerController.class);

    private KafkaTemplate kafkaTemplate;
    private final String TOPIC = "my-first-topic";

    public ProducerController(KafkaTemplate kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("messages/{message}")
    public String sendMessage(@PathVariable("message") String message) {
        logger.info(">>>> Sending message : " + message + " to TOPIC: " + TOPIC);
        kafkaTemplate.send(TOPIC, message);
        return "Message sent successful";
    }
}
