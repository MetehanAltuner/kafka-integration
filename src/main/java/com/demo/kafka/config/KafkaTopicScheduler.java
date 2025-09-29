package com.demo.kafka.config;

import com.demo.kafka.feature.topic.Topic;
import com.demo.kafka.feature.topic.TopicRepository;
import com.demo.kafka.service.DynamicKafkaConsumer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class KafkaTopicScheduler {

    private final TopicRepository topicRepository;
    private final DynamicKafkaConsumer kafkaConsumer;

    public KafkaTopicScheduler(TopicRepository topicRepository, DynamicKafkaConsumer kafkaConsumer) {
        this.topicRepository = topicRepository;
        this.kafkaConsumer = kafkaConsumer;
    }

    @Scheduled(fixedRate = 30000) // Her 30 saniyede bir çalışır
    public void checkAndSubscribeTopics() {
        List<String> topics = topicRepository.findAll()
                .stream()
                .map(Topic::getName)
                .collect(Collectors.toList());

        kafkaConsumer.subscribeToTopics(topics);
    }
}
