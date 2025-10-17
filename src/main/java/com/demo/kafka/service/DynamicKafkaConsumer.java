package com.demo.kafka.service;

import com.demo.kafka.common.helper.DbOpsHelper;
import com.demo.kafka.common.kafka.TopicAutoCreator;
import com.demo.kafka.feature.database.Database;
import com.demo.kafka.feature.mapping.Mapping;
import com.demo.kafka.feature.mapping.MappingRepository;
import com.demo.kafka.feature.tables.Tables;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class DynamicKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DynamicKafkaConsumer.class);

    private final MappingRepository mappingsRepository;
    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;
    private final ObjectMapper objectMapper;
    private final Map<String, MessageListenerContainer> activeListeners = new ConcurrentHashMap<>();
    private final DbOpsHelper db;
    private final TopicAutoCreator topicAutoCreator;

    public DynamicKafkaConsumer(MappingRepository mappingsRepository,
                                ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
                                DbOpsHelper db, TopicAutoCreator topicAutoCreator) {
        this.mappingsRepository = mappingsRepository;
        this.containerFactory = containerFactory;
        this.topicAutoCreator = topicAutoCreator;
        this.objectMapper = new ObjectMapper();
        this.db = db;
    }

    public void subscribeToTopics(List<String> topics) {
        for (String topic : topics) {
            if (!activeListeners.containsKey(topic)) {

                topicAutoCreator.ensureDltExists(topic);

                var container = containerFactory.createContainer(topic);
                container.setBeanName("dyn-" + topic);
                container.getContainerProperties().setGroupId("dynamic-group");
                container.getContainerProperties().setMissingTopicsFatal(false);
                container.getContainerProperties().setAuthExceptionRetryInterval(Duration.ofSeconds(30));
                container.getContainerProperties().setIdleEventInterval(300_000L); // 5 dakika

                container.setupMessageListener((MessageListener<String, String>) record -> {
                    if (record.value() == null) {
                        logger.debug("Skipping tombstone for key {}", record.key());
                        return;
                    }
                    try {

                        processMessage(topic, record);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                container.start();
                activeListeners.put(topic, container);
                logger.info("Subscribed to topic: {}", topic);
            }
        }
    }

    public void processMessage(String topic, ConsumerRecord<String, String> record) throws Exception {
        logger.debug("Processing Topic: {} | Key: {} | Partition: {} | Offset: {} | Value: {}",
                topic, record.key(), record.partition(), record.offset(), record.value());

        try {
            JsonNode rootNode = objectMapper.readTree(record.value());

            String operation = rootNode.get("op").asText();
            JsonNode after = rootNode.get("after");
            JsonNode before = rootNode.get("before");

            List<Mapping> mappings = mappingsRepository.findByTopicName(topic);

            Map<Database, Map<Tables, List<Mapping>>> groupedMappings = mappings.stream()
                    .collect(Collectors.groupingBy(
                            m -> m.getTargetColumn().getTable().getDatabase(),
                            Collectors.groupingBy(m -> m.getTargetColumn().getTable())
                    ));

            for (Map.Entry<Database, Map<Tables, List<Mapping>>> databaseEntry : groupedMappings.entrySet()) {
                Database database = databaseEntry.getKey();
                Map<Tables, List<Mapping>> tableMappings = databaseEntry.getValue();

                for (Map.Entry<Tables, List<Mapping>> tableEntry : tableMappings.entrySet()) {
                    Tables table = tableEntry.getKey();
                    List<Mapping> columnMappings = tableEntry.getValue();

                    switch (operation) {
                        case "c":
                            if (after != null) handleInsert(database, table, columnMappings, after);
                            break;
                        case "u":
                            if (after != null) handleUpdate(database, table, columnMappings, before, after);
                            break;
                        case "d":
                            if (before != null) handleDelete(database, table, columnMappings, before);
                            break;
                        case "r":
                            if (after != null) handleSnapshotUpsert(database, table, columnMappings, after);
                            break;
                        default:
                            logger.warn("Unknown operation: {}", operation);
                    }
                }
            }

            logger.debug("Message processed OK (topic={}, offset={})", topic, record.offset());
        } catch (Exception e) {
            logger.error("Error processing message from topic: {}, value: {}", topic, record.value(), e);
            throw e;
        }
    }

    // ---- Operation handlers ----

    private void handleSnapshotUpsert(Database database, Tables table, List<Mapping> columnMappings, JsonNode after) {
        // INSERT değerleri
        var insertValues = new java.util.HashMap<String, Object>();
        for (Mapping mapping : columnMappings) {
            String src = mapping.getSourceColumn().getName();
            String tgt = mapping.getTargetColumn().getName();
            Object raw = db.extractValue(after, src, Object.class);
            Object val = db.coerceForTarget(database, table, tgt, raw);
            insertValues.put(tgt, val);
        }

        // PK değerleri
        List<Mapping> pkMappings = DbOpsHelper.getPrimaryKeyMappings(columnMappings);
        var pkValues = new java.util.HashMap<String, Object>();
        for (Mapping pkMap : pkMappings) {
            String pkSourceName = pkMap.getSourceColumn().getName();
            String pkTarget = pkMap.getTargetColumn().getName();
            Object pkVal = db.extractValue(after, pkSourceName, Object.class);
            if (pkVal == null) {
                throw new IllegalArgumentException("Primary key value missing for column: " + pkTarget);
            }
            pkValues.put(pkTarget, pkVal);
        }

        boolean exists = db.recordExists(database, table, columnMappings, insertValues);
        if (!exists) {
            db.performInsert(database, table, insertValues);
        } else {
            var newValues = new java.util.HashMap<>(insertValues);
            for (String pk : pkValues.keySet()) newValues.remove(pk);
            if (newValues.isEmpty()) {
                logger.debug("SNAPSHOT UPSERT skipped: no non-PK columns to update for {}", table.getName());
                return;
            }
            db.performUpdate(database, table, newValues, pkValues);
        }
    }

    private void handleInsert(Database database, Tables table, List<Mapping> columnMappings, JsonNode after) {
        var values = new java.util.HashMap<String, Object>();
        for (Mapping mapping : columnMappings) {
            String src = mapping.getSourceColumn().getName();
            String tgt = mapping.getTargetColumn().getName();
            Object raw = db.extractValue(after, src, Object.class);
            Object val = db.coerceForTarget(database, table, tgt, raw);
            values.put(tgt, val);
        }
        if (db.recordExists(database, table, columnMappings, values)) {
            logger.warn("Record with primary key already exists in table {}", table.getName());
            return;
        }
        db.performInsert(database, table, values);
    }

    private void handleUpdate(Database database, Tables table, List<Mapping> columnMappings,
                              JsonNode before, JsonNode after) {
        // SET değerleri
        var newValues = new java.util.HashMap<String, Object>();
        for (Mapping mapping : columnMappings) {
            String src = mapping.getSourceColumn().getName();
            String tgt = mapping.getTargetColumn().getName();
            Object raw = db.extractValue(after, src, Object.class);
            Object val = db.coerceForTarget(database, table, tgt, raw);
            newValues.put(tgt, val);
        }

        // PK değerleri
        List<Mapping> pkMappings = DbOpsHelper.getPrimaryKeyMappings(columnMappings);
        var pkValues = new java.util.HashMap<String, Object>();
        for (Mapping pkMap : pkMappings) {
            String pkSourceName = pkMap.getSourceColumn().getName();
            String pkTarget = pkMap.getTargetColumn().getName();
            Object pkVal = db.extractValue(after, pkSourceName, Object.class);
            if (pkVal == null) pkVal = db.extractValue(before, pkSourceName, Object.class);
            if (pkVal == null) {
                throw new IllegalArgumentException("Primary key value missing for column: " + pkTarget);
            }
            pkValues.put(pkTarget, pkVal);
            newValues.remove(pkTarget);
        }

        db.performUpdate(database, table, newValues, pkValues);
    }

    private void handleDelete(Database database, Tables table, List<Mapping> columnMappings, JsonNode before) {
        List<Mapping> pkMappings = DbOpsHelper.getPrimaryKeyMappings(columnMappings);
        var pkValues = new java.util.HashMap<String, Object>();
        for (Mapping pkMap : pkMappings) {
            String pkSourceName = pkMap.getSourceColumn().getName();
            String pkTarget = pkMap.getTargetColumn().getName();
            Object pkVal = db.extractValue(before, pkSourceName, Object.class);
            if (pkVal == null) {
                throw new IllegalArgumentException("Primary key value missing for column: " + pkTarget);
            }
            pkValues.put(pkTarget, pkVal);
        }
        db.performDelete(database, table, pkValues);
    }
}
