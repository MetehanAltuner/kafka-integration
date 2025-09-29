package com.demo.kafka.feature.mapping.dto;

import com.demo.kafka.feature.mapping.Mapping;

public class MappingResponseDto {

    private Long id;
    private Long topicId;

    // ESKI: private String sourceColumn;
    // YENI: hem id hem ad
    private Long sourceColumnId;
    private String sourceColumnName;

    private Long targetColumnId;
    private String targetColumnName; // opsiyonel ama pratik

    // Getters & Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public Long getTopicId() { return topicId; }
    public void setTopicId(Long topicId) { this.topicId = topicId; }

    public Long getSourceColumnId() { return sourceColumnId; }
    public void setSourceColumnId(Long sourceColumnId) { this.sourceColumnId = sourceColumnId; }

    public String getSourceColumnName() { return sourceColumnName; }
    public void setSourceColumnName(String sourceColumnName) { this.sourceColumnName = sourceColumnName; }

    public Long getTargetColumnId() { return targetColumnId; }
    public void setTargetColumnId(Long targetColumnId) { this.targetColumnId = targetColumnId; }

    public String getTargetColumnName() { return targetColumnName; }
    public void setTargetColumnName(String targetColumnName) { this.targetColumnName = targetColumnName; }

    public static MappingResponseDto fromEntity(Mapping mapping) {
        MappingResponseDto dto = new MappingResponseDto();
        dto.setId(mapping.getId());
        dto.setTopicId(mapping.getTopic() != null ? mapping.getTopic().getId() : null);

        if (mapping.getSourceColumn() != null) {
            dto.setSourceColumnId(mapping.getSourceColumn().getId());
            dto.setSourceColumnName(mapping.getSourceColumn().getName());
        }

        if (mapping.getTargetColumn() != null) {
            dto.setTargetColumnId(mapping.getTargetColumn().getId());
            dto.setTargetColumnName(mapping.getTargetColumn().getName());
        }

        return dto;
    }
}
