package com.demo.kafka.feature.mapping.dto;

import com.demo.kafka.feature.columns.Columns;
import com.demo.kafka.feature.mapping.Mapping;
import com.demo.kafka.feature.topic.Topic;

public class MappingDto {

    private Long id;
    private Topic topic;
    private Columns sourceColumn;
    private Columns targetColumn;
    private boolean primaryKey;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Topic getTopic() { return topic; }
    public void setTopic(Topic topic) { this.topic = topic; }

    public Columns getSourceColumn() { return sourceColumn; }
    public void setSourceColumn(Columns sourceColumn) { this.sourceColumn = sourceColumn; }

    public Columns getTargetColumn() { return targetColumn; }
    public void setTargetColumn(Columns targetColumn) { this.targetColumn = targetColumn; }

    public boolean isPrimaryKey() { return primaryKey; }
    public void setPrimaryKey(boolean primaryKey) { this.primaryKey = primaryKey; }

    public Long getSourceColumnId() { return sourceColumn != null ? sourceColumn.getId() : null; }
    public String getSourceColumnName() { return sourceColumn != null ? sourceColumn.getName() : null; }
    public Long getTargetColumnId() { return targetColumn != null ? targetColumn.getId() : null; }
    public String getTargetColumnName() { return targetColumn != null ? targetColumn.getName() : null; }

    public static MappingDto fromEntity(Mapping mapping) {
        MappingDto dto = new MappingDto();
        dto.setId(mapping.getId());
        dto.setTopic(mapping.getTopic());
        dto.setSourceColumn(mapping.getSourceColumn());
        dto.setTargetColumn(mapping.getTargetColumn());
        dto.setPrimaryKey(mapping.getTargetColumn() != null && mapping.getTargetColumn().isPrimaryKey());
        return dto;
    }

    public Mapping toEntity() {
        Mapping mapping = new Mapping();
        mapping.setId(this.id);
        mapping.setTopic(this.topic);
        mapping.setSourceColumn(this.sourceColumn);
        mapping.setTargetColumn(this.targetColumn);
        return mapping;
    }
}
