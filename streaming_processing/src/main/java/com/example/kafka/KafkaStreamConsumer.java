package com.example.kafka;

import com.example.dto.CsvRecordDTO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamConsumer implements Serializable {
    private final KafkaSource<String> kafkaSource;
    private static final String JOB_NAME = "kafka-stream-consumer";

    @PostConstruct
    public void start() {
        try {
            // Создаем конфигурацию Flink
            Configuration configuration = new Configuration();
            
            // Создаем окружение выполнения Flink с конфигурацией
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

            // Создаем поток данных
            DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            // Добавляем обработку и вывод в консоль
            stream.map(new RichMapFunction<String, CsvRecordDTO>() {
                private transient ObjectMapper objectMapper;

                @Override
                public void open(Configuration parameters) {
                    objectMapper = new ObjectMapper();
                    objectMapper.registerModule(new JavaTimeModule());
                }

                @Override
                public CsvRecordDTO map(String jsonString) throws Exception {
                    try {
                        return objectMapper.readValue(jsonString, CsvRecordDTO.class);
                    } catch (Exception e) {
                        log.error("Error deserializing JSON: {}", jsonString, e);
                        throw e;
                    }
                }
            }).addSink(new SinkFunction<>() {
                @Override
                public void invoke(CsvRecordDTO value, Context context) {
                    log.info("Received record: {}", value);
                }
            });

            // Запускаем выполнение
            env.execute(JOB_NAME);
        } catch (Exception e) {
            log.error("Error running Flink job", e);
            throw new RuntimeException("Failed to start Flink job", e);
        }
    }
} 