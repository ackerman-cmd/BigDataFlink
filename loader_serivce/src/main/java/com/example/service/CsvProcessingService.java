package com.example.service;

import com.example.dto.CsvRecordDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.bean.HeaderColumnNameMappingStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.example.validation.ValidationUtil.isValidRecord;

@Slf4j
@Service
@RequiredArgsConstructor
public class CsvProcessingService {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${file.source.path}")
    private String sourcePath;

    @Value("${kafka.topic}")
    private String topic;

    private static final int THREAD_POOL_SIZE = 10;
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("M/d/yyyy");
    private static final AtomicLong counter = new AtomicLong(1);

    public void processFiles() {
        log.info("Starting CSV processing from directory: {}", sourcePath);
        try {
            Path directory = Paths.get(sourcePath);
            if (!Files.isDirectory(directory)) {
                log.error("Path {} is not a directory", sourcePath);
                return;
            }

            ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
            try {
                Files.list(directory)
                        .filter(path -> path.toString().endsWith(".csv"))
                        .forEach(path -> executorService.submit(() -> processFile(path)));

                executorService.shutdown();
                if (!executorService.awaitTermination(2, TimeUnit.HOURS)) {
                    log.warn("Processing did not complete within the timeout period");
                    executorService.shutdownNow();
                }
            } catch (Exception e) {
                log.error("Error processing CSV files", e);
                executorService.shutdownNow();
            }
        } catch (Exception e) {
            log.error("Error accessing directory: {}", sourcePath, e);
        }
    }

    private void processFile(Path filePath) {
        log.info("Processing file: {}", filePath);
        long processedLines = 0;
        long skippedLines = 0;

        try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.UTF_8)) {
            if (!reader.ready()) {
                log.warn("File {} is empty or unreadable", filePath);
                return;
            }

            HeaderColumnNameMappingStrategy<CsvRecordDTO> strategy = new HeaderColumnNameMappingStrategy<>();
            strategy.setType(CsvRecordDTO.class);

            CSVParser parser = new CSVParserBuilder()
                    .withSeparator(',')
                    .withQuoteChar('"')
                    .build();

            CSVReader csvReader = new CSVReaderBuilder(reader)
                    .withCSVParser(parser)
                    .build();

            CsvToBean<CsvRecordDTO> csvToBean = new CsvToBeanBuilder<CsvRecordDTO>(csvReader)
                    .withType(CsvRecordDTO.class)
                    .withMappingStrategy(strategy)
                    .withIgnoreEmptyLine(true)
                    .withThrowExceptions(false)
                    .build();

            for (CsvRecordDTO dto : csvToBean) {
                try {
                    dto.setId(counter.getAndIncrement());
                    if (!isValidRecord(dto)) {
                        log.warn("Invalid record in file {}: {}. Skipping.", filePath, dto);
                        skippedLines++;
                        continue;
                    }
                    String json = objectMapper.writeValueAsString(dto);
                    sendToKafka(json, filePath, processedLines + 1);
                    processedLines++;

                    if (processedLines % 1000 == 0) {
                        log.info("Processed {} lines from file: {}", processedLines, filePath);
                    }
                } catch (Exception e) {
                    log.error("Error processing record in file {}: {}", filePath, dto, e);
                    skippedLines++;
                }
            }

            csvToBean.getCapturedExceptions().forEach(e ->
                    log.error("Parsing error in file {} at line {}: {}",
                            filePath, e.getLineNumber(), e.getMessage()));

            log.info("Finished processing file: {}, total lines processed: {}, skipped: {}",
                    filePath, processedLines, skippedLines);
        } catch (Exception e) {
            log.error("Error reading file: {}", filePath, e);
        }
    }

    private void sendToKafka(String json, Path filePath, long lineNumber) {
        log.info("Preparing to send to Kafka from file {} (line {}): {}", filePath, lineNumber, json); // Лог JSON перед отправкой
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, json);
        future.whenComplete((result, throwable) -> {
            if (throwable == null) {
                RecordMetadata metadata = result.getRecordMetadata();
                log.info("Successfully sent record from file {} (line {}): topic={}, partition={}, offset={}, timestamp={}",
                        filePath, lineNumber, metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                log.error("Failed to send record from file {} (line {}): {}", filePath, lineNumber, throwable.getMessage(), throwable);
            }
        }).exceptionally(throwable -> {
            log.error("Exception while sending record from file {} (line {}): {}", filePath, lineNumber, throwable.getMessage(), throwable);
            return null;
        });
    }


}