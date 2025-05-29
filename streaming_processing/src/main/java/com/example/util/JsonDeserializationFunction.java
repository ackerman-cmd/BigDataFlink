package com.example.util;

import com.example.dto.CsvRecordDTO;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
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
        log.info("Инициализация JsonDeserializationFunction");
        objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(new SimpleDateFormat("M/d/yyyy"));
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    }

    @Override
    public void processElement(String jsonString, Context ctx, Collector<CsvRecordDTO> out) {
        try {
            if (jsonString == null || jsonString.trim().isEmpty()) {
                log.warn("Получен пустой или null JSON: {}", jsonString);
                return;
            }
            log.info("Обработка JSON: {}", jsonString);
            CsvRecordDTO record = objectMapper.readValue(jsonString, CsvRecordDTO.class);
            if (record == null) {
                log.warn("Десериализованная запись null для JSON: {}", jsonString);
                return;
            }
            log.info("Успешно десериализован JSON в CsvRecordDTO: id={}, sale_customer_id={}, sale_seller_id={}, sale_product_id={}, product_category={}",
                    record.getId(), record.getSaleCustomerId(), record.getSaleSellerId(), record.getSaleProductId(), record.getProductCategory());
            // Проверка критических полей
            if (record.getId() == null || record.getSaleCustomerId() == null || record.getSaleSellerId() == null ||
                    record.getSaleProductId() == null || record.getProductCategory() == null) {
                log.warn("Отсутствуют критические поля в CsvRecordDTO: id={}, sale_customer_id={}, sale_seller_id={}, sale_product_id={}, product_category={}",
                        record.getId(), record.getSaleCustomerId(), record.getSaleSellerId(), record.getSaleProductId(), record.getProductCategory());
                return;
            }
            out.collect(record);
        } catch (InvalidFormatException e) {
            String fieldName = e.getPath() != null && !e.getPath().isEmpty() ? e.getPath().get(0).getFieldName() : "неизвестно";
            String value = e.getValue() != null ? e.getValue().toString() : "null";
            log.error("Неверный формат для поля '{}'. Значение: '{}'. Ожидаемый тип: {}. JSON: {}. Ошибка: {}",
                    fieldName, value, e.getTargetType(), jsonString, e.getMessage(), e);
        } catch (MismatchedInputException e) {
            String fieldName = e.getPath() != null && !e.getPath().isEmpty() ? e.getPath().get(0).getFieldName() : "неизвестно";
            log.error("Несоответствие ввода для поля '{}'. JSON: {}. Ошибка: {}", fieldName, jsonString, e.getMessage(), e);
        } catch (Exception e) {
            log.error("Ошибка десериализации JSON: {}. Причина: {}, Сообщение: {}", jsonString, e.getCause(), e.getMessage(), e);
        }
    }
}