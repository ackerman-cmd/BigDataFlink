package com.example.util;

import com.example.dto.CsvRecordDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.text.SimpleDateFormat;

@Slf4j
public class JsonDeserializationFunction extends ProcessFunction<String, CsvRecordDTO> implements Serializable {
    private transient ObjectMapper objectMapper;
    @Override
    public void processElement(String jsonString, Context ctx, Collector<CsvRecordDTO> out) {
        try {
            log.debug("Processing JSON: {}", jsonString);
            CsvRecordDTO record = objectMapper.readValue(jsonString, CsvRecordDTO.class);
            out.collect(record);
        } catch (Exception e) {
            log.error("Error deserializing JSON: {}", jsonString, e);
            throw new RuntimeException("Failed to deserialize JSON", e); // Для диагностики
        }
    }

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("M/d/yyyy")); // Matches the JSON date format
        objectMapper.registerModule(new JavaTimeModule()); // Optional, only if other java.time types are used elsewhere
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }
}
