package com.demo.kafka.config;

import com.demo.kafka.feature.database.Database;
import jakarta.annotation.PreDestroy;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.Persistence;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class DatabaseConnectionUtil {

    private final Map<String, EntityManagerFactory> emfCache = new ConcurrentHashMap<>();

    private String key(Database db, String user) {
        return db.getConnectionUrl() + "|" + user;
    }

    public EntityManager createEntityManager(Database database) {
        String user = System.getenv("kafkadb.username");
        String pass = System.getenv("kafkadb.password");

        if (user == null || user.isBlank() || pass == null || pass.isBlank()) {
            throw new IllegalStateException(
                    "Environment variables kafkadb.username and kafkadb.password must be set.");
        }
        String key = key(database, user);
        EntityManagerFactory emf = emfCache.computeIfAbsent(key, k -> buildEmf(database, user, pass));
        return emf.createEntityManager();
    }

    private EntityManagerFactory buildEmf(Database db, String user, String pass) {
        Map<String, Object> p = new HashMap<>();

        // --- Hibernate + HikariCP ---
        p.put("hibernate.connection.provider_class",
                "org.hibernate.hikaricp.internal.HikariCPConnectionProvider");

        // Hikari parametreleri
        p.put("hibernate.hikari.jdbcUrl", db.getConnectionUrl());
        p.put("hibernate.hikari.username", user);
        p.put("hibernate.hikari.password", pass);
        p.put("hibernate.hikari.maximumPoolSize", "10");     // Maksimum 10 bağlantı
        p.put("hibernate.hikari.minimumIdle", "1");
        p.put("hibernate.hikari.idleTimeout", "300000");     // 5 dk
        p.put("hibernate.hikari.maxLifetime", "1800000");    // 30 dk
        p.put("hibernate.hikari.connectionTimeout", "30000");// 30 sn
        p.put("hibernate.hikari.poolName", "dyn-" + Math.abs(Objects.hash(db.getConnectionUrl(), user)));

        p.put("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        p.put("hibernate.jdbc.lob.non_contextual_creation", "true");
        p.put("hibernate.temp.use_jdbc_metadata_defaults", "false");

        return Persistence.createEntityManagerFactory("dynamic-persistence-unit", p);
    }

    @PreDestroy
    public void closeAll() {
        emfCache.values().forEach(emf -> {
            try { if (emf.isOpen()) emf.close(); } catch (Exception ignored) {}
        });
        emfCache.clear();
    }
}



