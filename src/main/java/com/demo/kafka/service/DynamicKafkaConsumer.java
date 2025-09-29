package com.demo.kafka.service;

import com.demo.kafka.config.DatabaseConnectionUtil;
import com.demo.kafka.feature.database.Database;
import com.demo.kafka.feature.database.DatabaseRepository;
import com.demo.kafka.feature.mapping.Mapping;
import com.demo.kafka.feature.mapping.MappingRepository;
import com.demo.kafka.feature.tables.Tables;
import com.demo.kafka.feature.tables.TablesRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class DynamicKafkaConsumer {

    private final MappingRepository mappingsRepository;
    private final TablesRepository tableRepository;
    private final DatabaseRepository databaseRepository;
    private final ConcurrentKafkaListenerContainerFactory<String, String> containerFactory;
    private final ObjectMapper objectMapper;
    private final Map<String, MessageListenerContainer> activeListeners = new HashMap<>();
    private final DatabaseConnectionUtil dbUtil;
    private final Map<String, String> columnTypeCache = new HashMap<>();
    private static boolean isTimestamp(String t) { return t != null && t.contains("timestamp"); }
    private static boolean isDate(String t)      { return t != null && t.equals("date"); }
    private static boolean isTime(String t)      { return t != null && t.startsWith("time") && !t.contains("stamp"); }
    private static boolean isBool(String t)      { return t != null && t.equals("boolean"); }
    private static boolean isIntLike(String t)   { return t != null && (t.startsWith("int") || t.equals("smallint") || t.equals("bigint")); }
    private static boolean isNumeric(String t)   { return t != null && (t.equals("numeric") || t.equals("decimal")); }
    private static boolean isUuid(String t)      { return t != null && t.equals("uuid"); }

    private static final Logger logger = LoggerFactory.getLogger(DynamicKafkaConsumer.class);

    public DynamicKafkaConsumer(MappingRepository mappingsRepository,
                                TablesRepository tableRepository,
                                DatabaseRepository databaseRepository,
                                ConcurrentKafkaListenerContainerFactory<String, String> containerFactory,
                                DatabaseConnectionUtil dbUtil) {
        this.mappingsRepository = mappingsRepository;
        this.tableRepository = tableRepository;
        this.databaseRepository = databaseRepository;
        this.containerFactory = containerFactory;
        this.objectMapper = new ObjectMapper();
        this.dbUtil = dbUtil;
    }
    private String getColumnDataType(Database db, String schema, String table, String column) {
        String key = db.getId() + "|" + schema + "|" + table + "|" + column;
        String cached = columnTypeCache.get(key);
        if (cached != null) return cached;

        EntityManager em = dbUtil.createEntityManager(db);
        try {
            String sql = """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name   = :table
              AND column_name  = :column
            """;
            Object r = em.createNativeQuery(sql)
                    .setParameter("schema", schema)
                    .setParameter("table", table)
                    .setParameter("column", column)
                    .getSingleResult();
            String dt = (r != null) ? r.toString().toLowerCase() : null;
            columnTypeCache.put(key, dt);
            return dt;
        } finally {
            em.close();
        }
    }
    private static long epochToMillis(long v) {
        long av = Math.abs(v);
        if (av >= 100_000_000_000_000L) return v / 1_000L; // micro → ms
        if (av >=   1_000_000_000_000L) return v;         // ms
        if (av >=       1_000_000_000L) return v * 1_000L; // sec → ms
        return v; // zaten ms varsay
    }

    /** Hedef tip adına göre ham değeri uygun JDBC tipine çevirir. */
    private Object coerceForTarget(Database db, Tables table, String targetColumn, Object raw) {
        if (raw == null) return null;
        String schema = resolveSchema(table);
        String dt = getColumnDataType(db, schema, table.getName(), targetColumn); // ← tip dinamik

        // Sayısal epoch/tarih
        if (raw instanceof Number) {
            long v = ((Number) raw).longValue();
            if (isTimestamp(dt)) return new java.sql.Timestamp(epochToMillis(v));
            if (isDate(dt))      return new java.sql.Date(epochToMillis(v));
            if (isTime(dt))      return new java.sql.Time(epochToMillis(v));
            if (isBool(dt))      return v != 0; // 0/1 → boolean
            if (isIntLike(dt))   return v;      // JDBC long/int handle eder
            if (isNumeric(dt))   return new java.math.BigDecimal(String.valueOf(raw));
            // başka tiplere de aynen geçebilir
            return raw;
        }

        // Metin ISO tarih gelebilir (Debezium config’ine bağlı)
        if (raw instanceof String s) {
            if (isTimestamp(dt)) {
                try {
                    // ISO-Z varsa
                    java.time.Instant ins = java.time.Instant.parse(s);
                    return java.sql.Timestamp.from(ins);
                } catch (Exception ignore) {
                    // ISO local datetime
                    try {
                        java.time.LocalDateTime ldt = java.time.LocalDateTime.parse(s);
                        return java.sql.Timestamp.valueOf(ldt);
                    } catch (Exception ignore2) { /* bırak, DB cast ederse eder */ }
                }
            } else if (isDate(dt)) {
                try {
                    return java.sql.Date.valueOf(java.time.LocalDate.parse(s));
                } catch (Exception ignore) {}
            } else if (isTime(dt)) {
                try {
                    return java.sql.Time.valueOf(java.time.LocalTime.parse(s));
                } catch (Exception ignore) {}
            } else if (isBool(dt)) {
                if ("true".equalsIgnoreCase(s) || "1".equals(s))  return true;
                if ("false".equalsIgnoreCase(s) || "0".equals(s)) return false;
            } else if (isNumeric(dt)) {
                try { return new java.math.BigDecimal(s); } catch (Exception ignore) {}
            } else if (isUuid(dt)) {
                try { return java.util.UUID.fromString(s); } catch (Exception ignore) {}
            }
            return s; // DB’ye string olarak gönder
        }

        // Boolean/Map/Array vs. özel haller
        if (raw instanceof Boolean b) {
            if (isBool(dt)) return b;
            if (isIntLike(dt)) return b ? 1 : 0; // bool→int
            // diğer tiplere string olarak bırak
            return b;
        }

        // JSON obje/array ise (ör. metadata): jsonb kolonsa stringle
        if (raw instanceof com.fasterxml.jackson.databind.JsonNode node) {
            if ("json".equals(dt) || "jsonb".equals(dt)) return node.toString();
            return node.toString(); // son çare
        }

        return raw;
    }


    public void subscribeToTopics(List<String> topics) {
        for (String topic : topics) {
            if (!activeListeners.containsKey(topic)) {
                var container = containerFactory.createContainer(topic);
                container.getContainerProperties().setGroupId("dynamic-group");

                container.setupMessageListener((MessageListener<String, String>) record -> {
                    // Tombstone kaydı (value=null) -> atla
                    if (record.value() == null) {
                        logger.debug("Skipping tombstone for key {}", record.key());
                        return;
                    }
                    processMessage(topic, record);
                });

                container.start();
                activeListeners.put(topic, container);
                System.out.println("Subscribed to topic: " + topic);
            }
        }
    }

    public void processMessage(String topic, ConsumerRecord<String, String> record) {
        System.out.println("Processing Topic: " + topic + " | Message: " + record.value());

        try {
            logger.info("Processing message from topic: {}, key: {}, value: {}", topic, record.key(), record.value());
            JsonNode rootNode = objectMapper.readTree(record.value());

            String operation = rootNode.get("op").asText();
            JsonNode after = rootNode.get("after");
            JsonNode before = rootNode.get("before");

            List<Mapping> mappings = mappingsRepository.findByTopicName(topic);

            Map<Database, Map<Tables, List<Mapping>>> groupedMappings = mappings.stream()
                    .collect(Collectors.groupingBy(
                            mapping -> mapping.getTargetColumn().getTable().getDatabase(),
                            Collectors.groupingBy(mapping -> mapping.getTargetColumn().getTable())
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

            logger.info("Message processed successfully: {}", record.value());
        } catch (Exception e) {
            logger.error("Error processing message from topic: {}, value: {}", topic, record.value(), e);
        }
    }

    // ---- Operation handlers ----

    private void handleSnapshotUpsert(Database database, Tables table, List<Mapping> columnMappings, JsonNode after) {
        Map<String, Object> insertValues = new HashMap<>();
        for (Mapping mapping : columnMappings) {
            String src = mapping.getSourceColumn().getName();
            String tgt = mapping.getTargetColumn().getName();
            Object raw = extractValue(after, src, Object.class);
            Object val = coerceForTarget(database, table, tgt, raw); // ← DÖNÜŞÜM
            insertValues.put(tgt, val);
        }

        List<Mapping> pkMappings = getPrimaryKeyMappings(columnMappings);
        Map<String, Object> pkValues = new HashMap<>();
        for (Mapping pkMap : pkMappings) {
            String pkSourceName = pkMap.getSourceColumn().getName();
            String pkTarget = pkMap.getTargetColumn().getName();
            Object pkVal = extractValue(after, pkSourceName, Object.class);
            if (pkVal == null) {
                throw new IllegalArgumentException("Primary key value missing for column: " + pkTarget);
            }
            pkValues.put(pkTarget, pkVal);
        }

        boolean exists = recordExists(database, table, columnMappings, insertValues);

        if (!exists) {
            performInsert(database, table, insertValues);
        } else {
            Map<String, Object> newValues = new HashMap<>(insertValues);
            for (String pk : pkValues.keySet()) {
                newValues.remove(pk);
            }

            if (newValues.isEmpty()) {
                logger.info("SNAPSHOT UPSERT skipped: no non-PK columns to update for table {}", table.getName());
                return;
            }

            performUpdate(database, table, newValues, pkValues);
        }
    }

    private void handleInsert(Database database, Tables table, List<Mapping> columnMappings, JsonNode after) {
        Map<String, Object> values = new HashMap<>();
        for (Mapping mapping : columnMappings) {
            String src = mapping.getSourceColumn().getName();
            String tgt = mapping.getTargetColumn().getName();
            Object raw = extractValue(after, src, Object.class);
            Object val = coerceForTarget(database, table, tgt, raw); // ← DÖNÜŞÜM
            values.put(tgt, val);
        }
        if (recordExists(database, table, columnMappings, values)) {
            logger.warn("Record with primary key already exists in table {}", table.getName());
            return;
        }
        performInsert(database, table, values);
    }

    private void handleUpdate(Database database, Tables table, List<Mapping> columnMappings,
                              JsonNode before, JsonNode after) {

        // SET değerleri
        Map<String, Object> newValues = new HashMap<>();
        for (Mapping mapping : columnMappings) {
            String src = mapping.getSourceColumn().getName();
            String tgt = mapping.getTargetColumn().getName();
            Object raw = extractValue(after, src, Object.class);
            Object val = coerceForTarget(database, table, tgt, raw); // ← DÖNÜŞÜM
            newValues.put(tgt, val);
        }

        // PK değerleri
        List<Mapping> pkMappings = getPrimaryKeyMappings(columnMappings);
        Map<String, Object> pkValues = new HashMap<>();
        for (Mapping pkMap : pkMappings) {
            String pkSourceName = pkMap.getSourceColumn().getName();      // JSON alan adı
            String pkTarget = pkMap.getTargetColumn().getName();          // tablo PK sütunu
            Object pkVal = extractValue(after, pkSourceName, Object.class);
            if (pkVal == null) {
                pkVal = extractValue(before, pkSourceName, Object.class);
            }
            if (pkVal == null) {
                throw new IllegalArgumentException("Primary key value missing for column: " + pkTarget);
            }
            pkValues.put(pkTarget, pkVal);

            // PK kolonlarını SET'ten çıkar
            newValues.remove(pkTarget);
        }

        performUpdate(database, table, newValues, pkValues);
    }

    private void handleDelete(Database database, Tables table, List<Mapping> columnMappings, JsonNode before) {
        List<Mapping> pkMappings = getPrimaryKeyMappings(columnMappings);
        Map<String, Object> pkValues = new HashMap<>();
        for (Mapping pkMap : pkMappings) {
            String pkSourceName = pkMap.getSourceColumn().getName();
            String pkTarget = pkMap.getTargetColumn().getName();
            Object pkVal = extractValue(before, pkSourceName, Object.class);
            if (pkVal == null) {
                throw new IllegalArgumentException("Primary key value missing for column: " + pkTarget);
            }
            pkValues.put(pkTarget, pkVal);
        }
        performDelete(database, table, pkValues);
    }

    // ---- DB ops ----

    private void performInsert(Database database, Tables table, Map<String, Object> values) {
        EntityManager em = dbUtil.createEntityManager(database);
        String schema = resolveSchema(table);

        try {
            em.getTransaction().begin();

            String columns = values.keySet().stream()
                    .map(DynamicKafkaConsumer::quoteIdent)   // <-- this:: değil
                    .collect(Collectors.joining(", "));
            String placeholders = values.keySet().stream()
                    .map(k -> ":" + k)
                    .collect(Collectors.joining(", "));

            String sql = "INSERT INTO " + quoteIdent(schema) + "." + quoteIdent(table.getName())
                    + " (" + columns + ") VALUES (" + placeholders + ")";
            Query q = em.createNativeQuery(sql);

            for (Map.Entry<String, Object> e : values.entrySet()) {
                q.setParameter(e.getKey(), e.getValue());
            }

            int inserted = q.executeUpdate();
            logger.info("INSERT Operation - Inserted Rows: {}", inserted);

            em.getTransaction().commit();
        } catch (Exception e) {
            if (em.getTransaction().isActive()) em.getTransaction().rollback();
            logger.error("Error occurred during INSERT operation on table: {}", table.getName(), e);
        } finally {
            em.close();
        }
    }

    private void performUpdate(Database database, Tables table, Map<String, Object> values, Map<String, Object> pkValues) {
        if (values.isEmpty()) {
            logger.info("UPDATE skipped: no non-PK columns to update for table {}", table.getName());
            return;
        }

        EntityManager em = dbUtil.createEntityManager(database);
        String schema = resolveSchema(table);
        try {
            em.getTransaction().begin();

            String setClause = values.keySet().stream()
                    .map(col -> quoteIdent(col) + " = :" + col)
                    .collect(Collectors.joining(", "));

            String whereClause = pkValues.keySet().stream()
                    .map(pk -> quoteIdent(pk) + " = :pk_" + pk)
                    .collect(Collectors.joining(" AND "));

            String sql = "UPDATE " + quoteIdent(schema) + "." + quoteIdent(table.getName())
                    + " SET " + setClause + " WHERE " + whereClause;

            Query q = em.createNativeQuery(sql);
            for (Map.Entry<String, Object> e : values.entrySet()) q.setParameter(e.getKey(), e.getValue());
            for (Map.Entry<String, Object> e : pkValues.entrySet()) q.setParameter("pk_" + e.getKey(), e.getValue());

            int updated = q.executeUpdate();
            logger.info("UPDATE {} - rows: {}", table.getName(), updated);

            em.getTransaction().commit();
        } catch (Exception e) {
            if (em.getTransaction().isActive()) em.getTransaction().rollback();
            logger.error("Error during UPDATE on table: {}", table.getName(), e);
        } finally {
            em.close();
        }
    }

    private void performDelete(Database database, Tables table, Map<String, Object> pkValues) {
        EntityManager em = dbUtil.createEntityManager(database);
        String schema = resolveSchema(table);
        try {
            em.getTransaction().begin();

            String whereClause = pkValues.keySet().stream()
                    .map(pk -> quoteIdent(pk) + " = :pk_" + pk)
                    .collect(Collectors.joining(" AND "));

            String sql = "DELETE FROM " + quoteIdent(schema) + "." + quoteIdent(table.getName())
                    + " WHERE " + whereClause;

            Query q = em.createNativeQuery(sql);
            for (Map.Entry<String, Object> e : pkValues.entrySet()) q.setParameter("pk_" + e.getKey(), e.getValue());

            int deleted = q.executeUpdate();
            logger.info("DELETE {} - rows: {}", table.getName(), deleted);

            em.getTransaction().commit();
        } catch (Exception e) {
            if (em.getTransaction().isActive()) em.getTransaction().rollback();
            logger.error("Error occurred during DELETE operation on table: {}", table.getName(), e);
        } finally {
            em.close();
        }
    }

    private boolean recordExists(Database database, Tables table, List<Mapping> columnMappings, Map<String, Object> values) {
        EntityManager em = dbUtil.createEntityManager(database);
        String schema = resolveSchema(table);
        try {
            List<Mapping> pkMappings = getPrimaryKeyMappings(columnMappings);

            // PK değerleri
            Map<String, Object> pkValues = new HashMap<>();
            for (Mapping pk : pkMappings) {
                String target = pk.getTargetColumn().getName();
                if (!values.containsKey(target)) {
                    throw new IllegalArgumentException("Primary key value missing for column: " + target);
                }
                pkValues.put(target, values.get(target));
            }

            String where = pkValues.keySet().stream()
                    .map(pk -> quoteIdent(pk) + " = :pk_" + pk)
                    .collect(Collectors.joining(" AND "));

            String sql = "SELECT COUNT(*) FROM " + quoteIdent(schema) + "." + quoteIdent(table.getName())
                    + " WHERE " + where;

            Query q = em.createNativeQuery(sql);
            for (Map.Entry<String, Object> e : pkValues.entrySet()) q.setParameter("pk_" + e.getKey(), e.getValue());

            long count = ((Number) q.getSingleResult()).longValue();
            return count > 0;
        } finally {
            em.close();
        }
    }

    // ---- Utils ----

    private Object extractValue(JsonNode node, String columnName, Class<?> targetType) {
        if (node != null && node.has(columnName)) {
            JsonNode valueNode = node.get(columnName);
            if (valueNode == null || valueNode.isNull()) return null;

            try {
                if (valueNode.isInt())       return valueNode.intValue();
                else if (valueNode.isLong()) return valueNode.longValue();
                else if (valueNode.isDouble()) return valueNode.doubleValue();
                else if (valueNode.isBoolean()) return valueNode.booleanValue();
                else if (valueNode.isTextual()) return valueNode.textValue();
                else if (valueNode.isNull())  return null;
                else throw new IllegalArgumentException("Unsupported JSON value type for column: " + columnName);
            } catch (Exception e) {
                throw new IllegalArgumentException("Cannot convert value: " + valueNode + " to type: " + targetType.getName(), e);
            }
        }
        return null;
    }

    private static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }

    private static String resolveSchema(Tables table) {
        // İstersen tabloda şema alanı varsa kullan; yoksa "public"
        // return table.getSchema() != null ? table.getSchema() : "public";
        return "public";
    }

    private List<Mapping> getPrimaryKeyMappings(List<Mapping> columnMappings) {
        List<Mapping> pks = columnMappings.stream()
                .filter(m -> m.getTargetColumn().isPrimaryKey())
                .toList();
        if (pks.isEmpty()) {
            throw new IllegalArgumentException("Primary key column(s) not found for table: " +
                    columnMappings.stream().findFirst().map(m -> m.getTargetColumn().getTable().getName()).orElse("?"));
        }
        return pks;
    }
}
