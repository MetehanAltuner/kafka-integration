package com.demo.kafka.feature.mapping.dto;

public class MappingRequestDto {

    private Long topicId;

    // ESKİ: private String sourceColumn;
    // YENİ: sadece kolon ID'si gelsin (FK)
    private Long sourceColumnId;

    private Long targetColumnId;

    // Getters & Setters
    public Long getTopicId() {
        return topicId;
    }

    public void setTopicId(Long topicId) {
        this.topicId = topicId;
    }

    public Long getSourceColumnId() {
        return sourceColumnId;
    }

    public void setSourceColumnId(Long sourceColumnId) {
        this.sourceColumnId = sourceColumnId;
    }

    public Long getTargetColumnId() {
        return targetColumnId;
    }

    public void setTargetColumnId(Long targetColumnId) {
        this.targetColumnId = targetColumnId;
    }
}
