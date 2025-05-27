package com.example.util;

import com.example.dto.CsvRecordDTO;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.text.SimpleDateFormat;

public class JsonDeserializationFunction extends ProcessFunction<String, CsvRecordDTO> implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(JsonDeserializationFunction.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("M/d/yyyy"));
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }

    @Override
    public void processElement(String jsonString, Context ctx, Collector<CsvRecordDTO> out) throws Exception {
        try {
            if (jsonString == null || jsonString.trim().isEmpty()) {
                log.warn("Received empty or null JSON: {}", jsonString);
                return;
            }
            log.debug("Processing JSON: {}", jsonString);
            CsvRecordDTO record = objectMapper.readValue(jsonString, CsvRecordDTO.class);
            if (record != null) {
                out.collect(record);
            } else {
                log.warn("Deserialized record is null for JSON: {}", jsonString);
            }
        } catch (Exception e) {
            log.error("Error deserializing JSON: {}. Cause: {}, StackTrace: {}", jsonString, e.getCause(), e.getStackTrace(), e);
            // Пропускаем ошибочную запись
        }
    }
}
