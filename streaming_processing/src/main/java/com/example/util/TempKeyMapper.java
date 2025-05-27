package com.example.util;

import com.example.dto.CsvRecordDTO;
import org.apache.flink.api.common.functions.MapFunction;

import java.io.Serializable;
import java.util.UUID;

public class TempKeyMapper implements MapFunction<CsvRecordDTO, CsvRecordDTO>, Serializable {
    @Override
    public CsvRecordDTO map(CsvRecordDTO record) {
        if (record != null) {
            record.setTempKey(UUID.randomUUID().toString());
        }
        return record;
    }
}
