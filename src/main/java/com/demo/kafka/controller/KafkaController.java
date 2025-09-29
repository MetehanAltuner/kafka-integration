package com.demo.kafka.controller;

import com.demo.kafka.service.KafkaConsumer;
import com.demo.kafka.service.KafkaProducer;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/kafka")
public class KafkaController {

    private final KafkaProducer kafkaProducer;

    private final KafkaConsumer kafkaConsumer;

    public KafkaController(KafkaProducer kafkaProducer, KafkaConsumer kafkaConsumer) {
        this.kafkaProducer = kafkaProducer;
        this.kafkaConsumer = kafkaConsumer;
    }

    @PostMapping("/send/{topic}")
    public String sendMessage(@PathVariable String topic, @RequestBody String message) {
        kafkaProducer.sendMessage(topic, message);
        return "Mesaj gönderilen Kafka topic: " + topic;
    }

}
