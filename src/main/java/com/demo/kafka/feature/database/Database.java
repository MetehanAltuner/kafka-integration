package com.demo.kafka.feature.database;

import com.demo.kafka.feature.tables.Tables;
import com.fasterxml.jackson.annotation.JsonIgnore;
import jakarta.persistence.*;
import java.util.List;

@Entity
@Table(name = "databases")
public class Database {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String name;

    @Column(nullable = false)
    private String connectionUrl;

    @OneToMany(mappedBy = "database", cascade = CascadeType.ALL)
    @JsonIgnore
    private List<Tables> tables;

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public void setConnectionUrl(String connectionUrl) {
        this.connectionUrl = connectionUrl;
    }

    public List<Tables> getTables() {
        return tables;
    }

    public void setTables(List<Tables> tables) {
        this.tables = tables;
    }
}
