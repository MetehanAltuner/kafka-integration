package com.demo.kafka.common.helper;

import com.demo.kafka.config.DatabaseConnectionUtil;
import com.demo.kafka.feature.database.Database;
import com.demo.kafka.feature.mapping.Mapping;
import com.demo.kafka.feature.tables.Tables;
import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Component
public class DbOpsHelper {

    private static final Logger logger = LoggerFactory.getLogger(DbOpsHelper.class);

    private final DatabaseConnectionUtil dbUtil;
    private final Map<String, String> columnTypeCache = new ConcurrentHashMap<>();

    public DbOpsHelper(DatabaseConnectionUtil dbUtil) {
        this.dbUtil = dbUtil;
    }

    /** ==== Type helpers ==== */
    private static boolean isTimestamp(String t){ return t!=null && t.contains("timestamp"); }
    private static boolean isDate(String t)     { return t!=null && t.equals("date"); }
    private static boolean isTime(String t)     { return t!=null && t.startsWith("time") && !t.contains("stamp"); }
    private static boolean isBool(String t)     { return t!=null && t.equals("boolean"); }
    private static boolean isIntLike(String t)  { return t!=null && (t.startsWith("int") || t.equals("smallint") || t.equals("bigint")); }
    private static boolean isNumeric(String t)  { return t!=null && (t.equals("numeric") || t.equals("decimal")); }
    private static boolean isUuid(String t)     { return t!=null && t.equals("uuid"); }

    private static long epochToMillis(long v){
        long av = Math.abs(v);
        if (av >= 100_000_000_000_000L) return v / 1_000L; // micro -> ms
        if (av >=   1_000_000_000_000L) return v;           // ms
        if (av >=       1_000_000_000L) return v * 1_000L;  // sec -> ms
        return v;
    }

    /** ==== JSON -> Java basic extraction ==== */
    public Object extractValue(JsonNode node, String columnName, Class<?> targetType) {
        if (node != null && node.has(columnName)) {
            JsonNode v = node.get(columnName);
            if (v == null || v.isNull()) return null;
            if (v.isInt()) return v.intValue();
            if (v.isLong()) return v.longValue();
            if (v.isDouble()) return v.doubleValue();
            if (v.isBoolean()) return v.booleanValue();
            if (v.isTextual()) return v.textValue();
            if (v.isObject() || v.isArray()) return v; // json/jsonb kolonları için
            return v.toString();
        }
        return null;
    }

    /** ==== DB metadata ==== */
    public String getColumnDataType(Database db, String schema, String table, String column) {
        String key = db.getId() + "|" + schema + "|" + table + "|" + column;
        String cached = columnTypeCache.get(key);
        if (cached != null) return cached;

        EntityManager em = dbUtil.createEntityManager(db);
        try {
            Object r = em.createNativeQuery("""
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = :schema
                      AND table_name   = :table
                      AND column_name  = :column
                    """)
                    .setParameter("schema", schema)
                    .setParameter("table", table)
                    .setParameter("column", column)
                    .getSingleResult();
            String dt = (r != null) ? r.toString().toLowerCase() : null;
            columnTypeCache.put(key, dt);
            return dt;
        } finally { em.close(); }
    }

    /** ==== Type coercion ==== */
    public Object coerceForTarget(Database db, Tables table, String targetColumn, Object raw) {
        if (raw == null) return null;
        String schema = resolveSchema(table);
        String dt = getColumnDataType(db, schema, table.getName(), targetColumn);

        if (raw instanceof Number n) {
            long v = n.longValue();
            if (isTimestamp(dt)) return new Timestamp(epochToMillis(v));
            if (isDate(dt))      return new java.sql.Date(epochToMillis(v));
            if (isTime(dt))      return new Time(epochToMillis(v));
            if (isBool(dt))      return v != 0;
            if (isIntLike(dt))   return v;
            if (isNumeric(dt))   return new BigDecimal(String.valueOf(raw));
            return raw;
        }
        if (raw instanceof String s) {
            try {
                if (isTimestamp(dt)) {
                    try { return Timestamp.from(java.time.Instant.parse(s)); }
                    catch (Exception ignore) { return Timestamp.valueOf(java.time.LocalDateTime.parse(s)); }
                }
                if (isDate(dt)) return java.sql.Date.valueOf(java.time.LocalDate.parse(s));
                if (isTime(dt)) return java.sql.Time.valueOf(java.time.LocalTime.parse(s));
                if (isBool(dt)) return ("true".equalsIgnoreCase(s) || "1".equals(s));
                if (isNumeric(dt)) return new BigDecimal(s);
                if (isUuid(dt)) return java.util.UUID.fromString(s);
            } catch (Exception ignore) { /* bırak string gitsin */ }
            return s;
        }
        if (raw instanceof Boolean b) {
            if (isBool(dt)) return b;
            if (isIntLike(dt)) return b ? 1 : 0;
            return b;
        }
        if (raw instanceof JsonNode node) {
            // json/jsonb için stringle; değilse de string olarak dene
            return node.toString();
        }
        return raw;
    }

    /** ==== SQL helpers ==== */
    public static String quoteIdent(String name) {
        return "\"" + name.replace("\"", "\"\"") + "\"";
    }
    public static String resolveSchema(Tables table) {
        return "public"; // gelecekte table.getSchema() varsa burada kullan
    }
    public static List<Mapping> getPrimaryKeyMappings(List<Mapping> columnMappings) {
        List<Mapping> pks = columnMappings.stream()
                .filter(m -> m.getTargetColumn().isPrimaryKey())
                .toList();
        if (pks.isEmpty()) {
            throw new IllegalArgumentException("Primary key column(s) not found for table mapping");
        }
        return pks;
    }

    /** ==== DB ops ==== */
    public boolean recordExists(Database database, Tables table, List<Mapping> columnMappings, Map<String, Object> values) {
        EntityManager em = dbUtil.createEntityManager(database);
        String schema = resolveSchema(table);
        try {
            Map<String, Object> pkValues = new HashMap<>();
            for (Mapping pk : getPrimaryKeyMappings(columnMappings)) {
                String target = pk.getTargetColumn().getName();
                if (!values.containsKey(target)) {
                    throw new IllegalArgumentException("Primary key value missing for column: " + target);
                }
                pkValues.put(target, values.get(target));
            }
            String where = pkValues.keySet().stream()
                    .map(pk -> quoteIdent(pk) + " = :pk_" + pk)
                    .collect(Collectors.joining(" AND "));
            String sql = "SELECT 1 FROM " + quoteIdent(schema) + "." + quoteIdent(table.getName())
                    + " WHERE " + where + " LIMIT 1";

            Query q = em.createNativeQuery(sql);
            for (var e : pkValues.entrySet()) q.setParameter("pk_" + e.getKey(), e.getValue());
            return !q.getResultList().isEmpty();
        } finally { em.close(); }
    }

    public int performInsert(Database database, Tables table, Map<String, Object> values) {
        EntityManager em = dbUtil.createEntityManager(database);
        String schema = resolveSchema(table);
        try {
            em.getTransaction().begin();
            String columns = values.keySet().stream().map(DbOpsHelper::quoteIdent).collect(Collectors.joining(", "));
            String placeholders = values.keySet().stream().map(k -> ":" + k).collect(Collectors.joining(", "));
            String sql = "INSERT INTO " + quoteIdent(schema) + "." + quoteIdent(table.getName())
                    + " (" + columns + ") VALUES (" + placeholders + ")";
            Query q = em.createNativeQuery(sql);
            values.forEach(q::setParameter);
            int inserted = q.executeUpdate();
            em.getTransaction().commit();
            logger.debug("INSERT {} - rows: {}", table.getName(), inserted);
            return inserted;
        } catch (Exception e) {
            if (em.getTransaction().isActive()) em.getTransaction().rollback();
            logger.error("Error during INSERT: {}", table.getName(), e);
            throw e;
        } finally { em.close(); }
    }

    public int performUpdate(Database database, Tables table, Map<String, Object> values, Map<String, Object> pkValues) {
        if (values.isEmpty()) {
            logger.debug("UPDATE skipped: no non-PK columns to update for {}", table.getName());
            return 0;
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
            values.forEach(q::setParameter);
            pkValues.forEach((k,v) -> q.setParameter("pk_" + k, v));
            int updated = q.executeUpdate();
            em.getTransaction().commit();
            logger.debug("UPDATE {} - rows: {}", table.getName(), updated);
            return updated;
        } catch (Exception e) {
            if (em.getTransaction().isActive()) em.getTransaction().rollback();
            logger.error("Error during UPDATE: {}", table.getName(), e);
            throw e;
        } finally { em.close(); }
    }

    public int performDelete(Database database, Tables table, Map<String, Object> pkValues) {
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
            pkValues.forEach((k,v) -> q.setParameter("pk_" + k, v));
            int deleted = q.executeUpdate();
            em.getTransaction().commit();
            logger.debug("DELETE {} - rows: {}", table.getName(), deleted);
            return deleted;
        } catch (Exception e) {
            if (em.getTransaction().isActive()) em.getTransaction().rollback();
            logger.error("Error during DELETE: {}", table.getName(), e);
            throw e;
        } finally { em.close(); }
    }
}
