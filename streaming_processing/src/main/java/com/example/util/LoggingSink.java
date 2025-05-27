package com.example.util;

import com.example.dto.CsvRecordDTO;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.Serializable;

@Slf4j
public class LoggingSink implements SinkFunction<CsvRecordDTO>, Serializable {
    @Override
    public void invoke(CsvRecordDTO value, Context context) {
        log.info("Received record: {}", value);
    }
}
