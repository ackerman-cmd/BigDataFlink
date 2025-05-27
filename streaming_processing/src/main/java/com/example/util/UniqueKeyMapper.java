package com.example.util;

import com.example.dto.CsvRecordDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

public class UniqueKeyMapper implements MapFunction<CsvRecordDTO, CsvRecordDTO>, Serializable {
    private static final Logger log = LoggerFactory.getLogger(UniqueKeyMapper.class);

    @Override
    public CsvRecordDTO map(CsvRecordDTO record) throws Exception {
        if (record == null) {
            log.warn("Received null record in UniqueKeyMapper");
            return null;
        }
        try {
            record.setUniqueKey(UUID.randomUUID().toString());
            return record;
        } catch (Exception e) {
            log.error("Error setting unique key for record: {}", record, e);
            return null;
        }
    }
}