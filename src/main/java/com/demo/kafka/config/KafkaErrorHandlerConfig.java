package com.demo.kafka.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger logger = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(KafkaTemplate<String, String> kafkaTemplate) {

        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".dlt", record.partition())
        );
    }

    @Bean
    public DefaultErrorHandler defaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {

        DefaultErrorHandler h = new DefaultErrorHandler(recoverer, new FixedBackOff(2000L, 3L));
        h.addNotRetryableExceptions(
                IllegalArgumentException.class,
                org.springframework.dao.DataIntegrityViolationException.class,
                org.hibernate.exception.ConstraintViolationException.class,
                jakarta.persistence.PersistenceException.class
//                org.postgresql.util.PSQLException.class
        );
        h.setRetryListeners((record, ex, deliveryAttempt) ->
                logger.warn("Retry #{} for topic={}, partition={}, offset={}",
                        deliveryAttempt, record.topic(), record.partition(), record.offset(), ex));

        return h;
    }
    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            DefaultErrorHandler defaultErrorHandler
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(defaultErrorHandler);

        return factory;
    }
}
