package com.demo.kafka.feature.mapping;

import com.demo.kafka.common.exception.ResourceNotFoundException;
import com.demo.kafka.feature.columns.Columns;
import com.demo.kafka.feature.columns.ColumnsRepository;
import com.demo.kafka.feature.mapping.dto.MappingRequestDto;
import com.demo.kafka.feature.mapping.dto.MappingResponseDto;
import com.demo.kafka.feature.topic.Topic;
import com.demo.kafka.feature.topic.TopicRepository;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class MappingService {

    private final MappingRepository mappingRepository;
    private final TopicRepository topicRepository;
    private final ColumnsRepository columnsRepository;

    public MappingService(MappingRepository mappingRepository,
                          TopicRepository topicRepository,
                          ColumnsRepository columnsRepository) {
        this.mappingRepository = mappingRepository;
        this.topicRepository = topicRepository;
        this.columnsRepository = columnsRepository;
    }

    public MappingResponseDto createMapping(MappingRequestDto requestDto) {
        Topic topic = topicRepository.findById(requestDto.getTopicId())
                .orElseThrow(() -> new ResourceNotFoundException("Topic not found"));

        Columns sourceColumn = columnsRepository.findById(requestDto.getSourceColumnId())
                .orElseThrow(() -> new ResourceNotFoundException("Source Column not found"));

        Columns targetColumn = columnsRepository.findById(requestDto.getTargetColumnId())
                .orElseThrow(() -> new ResourceNotFoundException("Target Column not found"));

        Mapping mapping = new Mapping();
        mapping.setTopic(topic);
        mapping.setSourceColumn(sourceColumn);   // <-- Artık Columns (FK)
        mapping.setTargetColumn(targetColumn);

        Mapping savedMapping = mappingRepository.save(mapping);
        return MappingResponseDto.fromEntity(savedMapping);
    }

    public MappingResponseDto getMappingById(Long id) {
        Mapping mapping = mappingRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Mapping not found"));
        return MappingResponseDto.fromEntity(mapping);
    }

    public List<MappingResponseDto> getAllMappings() {
        return mappingRepository.findAll().stream()
                .map(MappingResponseDto::fromEntity)
                .collect(Collectors.toList());
    }

    public MappingResponseDto updateMapping(Long id, MappingRequestDto requestDto) {
        Mapping mapping = mappingRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Mapping not found"));

        Topic topic = topicRepository.findById(requestDto.getTopicId())
                .orElseThrow(() -> new ResourceNotFoundException("Topic not found"));

        Columns sourceColumn = columnsRepository.findById(requestDto.getSourceColumnId())
                .orElseThrow(() -> new ResourceNotFoundException("Source Column not found"));

        Columns targetColumn = columnsRepository.findById(requestDto.getTargetColumnId())
                .orElseThrow(() -> new ResourceNotFoundException("Target Column not found"));

        mapping.setTopic(topic);
        mapping.setSourceColumn(sourceColumn);   // <-- FK
        mapping.setTargetColumn(targetColumn);

        Mapping updatedMapping = mappingRepository.save(mapping);
        return MappingResponseDto.fromEntity(updatedMapping);
    }

    public void deleteMapping(Long id) {
        Mapping mapping = mappingRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Mapping not found"));
        mappingRepository.delete(mapping);
    }
}
