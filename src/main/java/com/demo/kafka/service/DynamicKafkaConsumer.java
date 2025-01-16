//package com.demo.kafka.service;
//
//import com.demo.kafka.config.DatabaseConnectionUtil;
//import com.demo.kafka.feature.database.Database;
//import com.demo.kafka.feature.database.DatabaseRepository;
//import com.demo.kafka.feature.mapping.Mapping;
//import com.demo.kafka.feature.mapping.MappingRepository;
//import com.demo.kafka.feature.tables.Tables;
//import com.demo.kafka.feature.tables.TablesRepository;
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import jakarta.persistence.EntityManager;
//import jakarta.persistence.Query;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.listener.MessageListener;
//import org.springframework.kafka.listener.MessageListenerContainer;
//import org.springframework.stereotype.Service;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//@Service
//public class DynamicKafkaConsumer {
//
//    private final MappingRepository mappingsRepository;
//    private final TablesRepository tableRepository;
//    private final DatabaseRepository databaseRepository;
//    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;
//    private final ObjectMapper objectMapper;
//    private final Map<String, MessageListenerContainer> activeListeners = new HashMap<>();
//
//    private static final Logger logger = LoggerFactory.getLogger(DynamicKafkaConsumer.class);
//
//    public DynamicKafkaConsumer(MappingRepository mappingsRepository,
//                                TablesRepository tableRepository,
//                                DatabaseRepository databaseRepository,
//                                ConcurrentKafkaListenerContainerFactory<String, String> containerFactory) {
//        this.mappingsRepository = mappingsRepository;
//        this.tableRepository = tableRepository;
//        this.databaseRepository = databaseRepository;
//        this.containerFactory = containerFactory;
//        this.objectMapper = new ObjectMapper();
//    }
//
//    public void subscribeToTopics(List<String> topics) {
//        for (String topic : topics) {
//            if (!activeListeners.containsKey(topic)) {
//                var container = containerFactory.createContainer(topic);
//                container.getContainerProperties().setGroupId("dynamic-group");
//
//                container.setupMessageListener((MessageListener<String, String>) record -> {
//                    processMessage(topic, record);
//                });
//
//                container.start();
//                activeListeners.put(topic, container);
//                System.out.println("Subscribed to topic: " + topic);
//            }
//        }
//    }
//
//    public void processMessage(String topic, ConsumerRecord<String, String> record) {
//        System.out.println("Processing Topic: " + topic + " | Message: " + record.value());
//
//        try {
//            logger.info("Processing message from topic: {}, key: {}, value: {}", topic, record.key(), record.value());
//            JsonNode rootNode = objectMapper.readTree(record.value());
//            JsonNode payload = rootNode.get("payload");
//
//            if (payload == null) {
//                logger.error("Payload is null. Skipping message...");
//                return;
//            }
//
//            String operation = payload.get("op").asText();
//            JsonNode after = payload.get("after");
//            JsonNode before = payload.get("before");
//
//            List<Mapping> mappings = mappingsRepository.findByTopicName(topic);
//
//            Map<Database, Map<Tables, List<Mapping>>> groupedMappings = mappings.stream()
//                    .collect(Collectors.groupingBy(
//                            mapping -> mapping.getTargetColumn().getTable().getDatabase(),
//                            Collectors.groupingBy(mapping -> mapping.getTargetColumn().getTable())
//                    ));
//
//            for (Map.Entry<Database, Map<Tables, List<Mapping>>> databaseEntry : groupedMappings.entrySet()) {
//                Database database = databaseEntry.getKey();
//                Map<Tables, List<Mapping>> tableMappings = databaseEntry.getValue();
//
//                for (Map.Entry<Tables, List<Mapping>> tableEntry : tableMappings.entrySet()) {
//                    Tables table = tableEntry.getKey();
//                    List<Mapping> columnMappings = tableEntry.getValue();
//
//                    switch (operation) {
//                        case "c":
//                            if (after != null) handleInsert(database, table, columnMappings, after);
//                            break;
//                        case "u":
//                            if (after != null) handleUpdate(database, table, columnMappings, before, after);
//                            break;
//                        case "d":
//                            if (before != null) handleDelete(database, table, columnMappings, before);
//                            break;
//                        default:
//                            logger.warn("Unknown operation: {}", operation);
//                    }
//                }
//            }
//
//            logger.info("Message processed successfully: {}", record.value());
//        } catch (Exception e) {
//            logger.error("Error processing message from topic: {}, value: {}", topic, record.value(), e);
//        }
//    }
//
//
//    private void handleInsert(Database database, Tables table, List<Mapping> columnMappings, JsonNode after) {
//        Map<String, Object> values = new HashMap<>();
//        for (Mapping mapping : columnMappings) {
//            String sourceColumn = mapping.getSourceColumn();
//            Object value = extractValue(after, sourceColumn, String.class);
//            values.put(mapping.getTargetColumn().getName(), value);
//        }
//        if (recordExists(database, table, columnMappings, values)) {
//            logger.warn("Record with primary key already exists in table {}", table.getName());
//            return;
//        }
//        performInsert(database, table, values);
//    }
//
//
//    private void handleUpdate(Database database, Tables table, List<Mapping> columnMappings, JsonNode before, JsonNode after) {
//        Map<String, Object> values = new HashMap<>();
//        for (Mapping mapping : columnMappings) {
//            String sourceColumn = mapping.getSourceColumn();
//            Object value = extractValue(after, sourceColumn, String.class);
//            values.put(mapping.getTargetColumn().getName(), value);
//        }
//        Long id = (Long) extractValue(after, "id", Long.class);
//        performUpdate(database, table, values, id);
//    }
//
//
//    private void handleDelete(Database database, Tables table, List<Mapping> columnMappings, JsonNode before) {
//        Long id = (Long) extractValue(before, "id", Long.class);
//        performDelete(database, table, id);
//    }
//
//    private void performInsert(Database database, Tables table, Map<String, Object> values) {
//        EntityManager entityManager = DatabaseConnectionUtil.createEntityManager(database);
//
//        try {
//            entityManager.getTransaction().begin();
//
//            String columns = String.join(", ", values.keySet());
//            String placeholders = values.keySet().stream().map(k -> ":" + k).collect(Collectors.joining(", "));
//
//            String sql = "INSERT INTO " + table.getName() + " (" + columns + ") VALUES (" + placeholders + ")";
//            Query query = entityManager.createNativeQuery(sql);
//
//            for (Map.Entry<String, Object> entry : values.entrySet()) {
//                query.setParameter(entry.getKey(), entry.getValue());
//            }
//
//            int inserted = query.executeUpdate();
//            logger.info("INSERT Operation - Inserted Rows: {}", inserted);
//
//            entityManager.getTransaction().commit();
//        } catch (Exception e) {
//            if (entityManager.getTransaction().isActive()) {
//                entityManager.getTransaction().rollback();
//            }
//            logger.error("Error occurred during INSERT operation on table: {}", table.getName(), e);
//        } finally {
//            entityManager.close();
//        }
//    }
//
//
//
//    private void performUpdate(Database database, Tables table, Map<String, Object> values, Long id) {
//        EntityManager entityManager = DatabaseConnectionUtil.createEntityManager(database);
//
//        try {
//            entityManager.getTransaction().begin();
//
//            String setClause = values.keySet().stream()
//                    .map(column -> column + " = :" + column)
//                    .collect(Collectors.joining(", "));
//
//            String sql = "UPDATE " + table.getName() + " SET " + setClause + " WHERE id = :id";
//            Query query = entityManager.createNativeQuery(sql);
//
//            for (Map.Entry<String, Object> entry : values.entrySet()) {
//                query.setParameter(entry.getKey(), entry.getValue());
//            }
//            query.setParameter("id", id);
//
//            int updated = query.executeUpdate();
//            logger.info("UPDATE Operation - Updated Rows: {}", updated);
//
//            entityManager.getTransaction().commit();
//        } catch (Exception e) {
//            if (entityManager.getTransaction().isActive()) {
//                entityManager.getTransaction().rollback();
//            }
//            logger.error("Error occurred during UPDATE operation on table: {}", table.getName(), e);
//        } finally {
//            entityManager.close();
//        }
//    }
//
//
//    private void performDelete(Database database, Tables table, Long id) {
//        EntityManager entityManager = DatabaseConnectionUtil.createEntityManager(database);
//
//        try {
//            entityManager.getTransaction().begin();
//
//            String sql = "DELETE FROM " + table.getName() + " WHERE id = :id";
//            Query query = entityManager.createNativeQuery(sql);
//            query.setParameter("id", id);
//
//            int deleted = query.executeUpdate();
//            logger.info("DELETE Operation - Deleted Rows: {}", deleted);
//
//            entityManager.getTransaction().commit();
//        } catch (Exception e) {
//            if (entityManager.getTransaction().isActive()) {
//                entityManager.getTransaction().rollback();
//            }
//            logger.error("Error occurred during DELETE operation on table: {}", table.getName(), e);
//        } finally {
//            entityManager.close();
//        }
//    }
//
//    private boolean recordExists(Database database, Tables table, List<Mapping> columnMappings, Map<String, Object> values) {
//        EntityManager entityManager = DatabaseConnectionUtil.createEntityManager(database);
//        try {
//            // Birincil anahtar sütununu buluyoruz.
//            String primaryKeyColumn = columnMappings.stream()
//                    .filter(mapping -> mapping.getTargetColumn().isPrimaryKey())
//                    .map(mapping -> mapping.getTargetColumn().getName())
//                    .findFirst()
//                    .orElseThrow(() -> new IllegalArgumentException("Primary key column not found for table: " + table.getName()));
//
//            if (!values.containsKey(primaryKeyColumn)) {
//                throw new IllegalArgumentException("Primary key value missing for column: " + primaryKeyColumn);
//            }
//
//            String sql = "SELECT COUNT(*) FROM " + table.getName() + " WHERE " + primaryKeyColumn + " = :primaryKeyValue";
//            Query query = entityManager.createNativeQuery(sql);
//            query.setParameter("primaryKeyValue", values.get(primaryKeyColumn));
//
//            Long count = ((Number) query.getSingleResult()).longValue();
//            return count > 0;
//        } finally {
//            entityManager.close();
//        }
//    }
//
//    private Object extractValue(JsonNode node, String columnName, Class<?> targetType) {
//    if (node.has(columnName)) {
//        JsonNode valueNode = node.get(columnName);
//        if (valueNode.isNull()) {
//            return null; // Değer null ise geri dön
//        }
//
//        try {
//            if (valueNode.isInt()) {
//                return valueNode.intValue();
//            } else if (valueNode.isLong()) {
//                return valueNode.longValue();
//            } else if (valueNode.isDouble()) {
//                return valueNode.doubleValue();
//            } else if (valueNode.isBoolean()) {
//                return valueNode.booleanValue();
//            } else if (valueNode.isTextual()) {
//                return valueNode.textValue();
//            } else if (valueNode.isNull()) {
//                return null;
//            } else {
//                throw new IllegalArgumentException("Unsupported JSON value type for column: " + columnName);
//            }
//        } catch (Exception e) {
//            throw new IllegalArgumentException("Cannot convert value: " + valueNode + " to type: " + targetType.getName(), e);
//        }
//    }
//    return null; // Eğer alan yoksa null döndür
//}
//
//}
