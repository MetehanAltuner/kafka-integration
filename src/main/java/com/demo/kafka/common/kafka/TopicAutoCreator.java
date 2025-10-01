package com.demo.kafka.common.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Component
public class TopicAutoCreator {

    private static final Logger logger = LoggerFactory.getLogger(TopicAutoCreator.class);
    private final KafkaAdmin kafkaAdmin;

    @Value("${app.dlt.partitions:1}")
    private int dltPartitions;

    @Value("${app.dlt.replication-factor:1}")
    private short dltReplication;

    public TopicAutoCreator(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    public void ensureDltExists(String sourceTopic) {
        String dlt = sourceTopic + ".dlt";
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            var topics = admin.listTopics().names().get(5, TimeUnit.SECONDS);
            if (!topics.contains(dlt)) {
                NewTopic newTopic = new NewTopic(dlt, dltPartitions, dltReplication);
                admin.createTopics(Collections.singletonList(newTopic)).all().get(5, TimeUnit.SECONDS);
            }
        } catch (Exception ignore) {
            logger.error("Failed to create DLT topic: " + dlt, ignore);
        }
    }
}
