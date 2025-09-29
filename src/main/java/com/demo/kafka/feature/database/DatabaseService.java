package com.demo.kafka.feature.database;

import com.demo.kafka.common.exception.ResourceNotFoundException;
import com.demo.kafka.feature.database.dto.DatabaseRequestDto;
import com.demo.kafka.feature.database.dto.DatabaseResponseDto;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class DatabaseService {

    private final DatabaseRepository databaseRepository;

    public DatabaseService(DatabaseRepository databaseRepository) {
        this.databaseRepository = databaseRepository;
    }

    public DatabaseResponseDto createDatabase(DatabaseRequestDto requestDto) {
        Database database = new Database();
        database.setName(requestDto.getName());
        database.setConnectionUrl(requestDto.getConnectionUrl());

        Database savedDatabase = databaseRepository.save(database);
        return DatabaseResponseDto.fromEntity(savedDatabase);
    }

    public DatabaseResponseDto getDatabaseById(Long id) {
        Database database = databaseRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found"));
        return DatabaseResponseDto.fromEntity(database);
    }

    public List<DatabaseResponseDto> getAllDatabases() {
        return databaseRepository.findAll().stream()
                .map(DatabaseResponseDto::fromEntity)
                .collect(Collectors.toList());
    }

    public DatabaseResponseDto updateDatabase(Long id, DatabaseRequestDto requestDto) {
        Database database = databaseRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found"));

        database.setName(requestDto.getName());
        database.setConnectionUrl(requestDto.getConnectionUrl());

        Database updatedDatabase = databaseRepository.save(database);
        return DatabaseResponseDto.fromEntity(updatedDatabase);
    }

    public void deleteDatabase(Long id) {
        Database database = databaseRepository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Database not found"));
        databaseRepository.delete(database);
    }
}

