package com.example.config;



import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    private static final String kafkaBootstrap = "kafka:9092";

    private static final String kafkaTopic = "data-topic";

    @Bean
    public Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBootstrap);
        properties.setProperty("group.id", "flink-consumer");

        return properties;
    }

    @Bean
    public KafkaSource<String> kafkaSource() {
        return KafkaSource.<String>builder()
                .setProperties(kafkaProperties())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setTopics(kafkaTopic)
                .build();
    }

    @Bean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
