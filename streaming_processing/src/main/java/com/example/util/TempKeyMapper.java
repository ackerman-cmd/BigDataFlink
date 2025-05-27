package com.example.util;

import com.example.dto.CsvRecordDTO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.UUID;

public class TempKeyMapper extends RichMapFunction<CsvRecordDTO, CsvRecordDTO> implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(TempKeyMapper.class);
    private transient MapState<String, String> keyState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<>(
                "keyState",
                String.class,
                String.class
        );
        keyState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public CsvRecordDTO map(CsvRecordDTO record) throws Exception {
        if (record == null) {
            log.warn("Received null record in TempKeyMapper");
            return null;
        }
        try {
            record.setTempKey(UUID.randomUUID().toString());
            record.setCustomerTempKey(getOrCreateUUID("customer_" + record.getSaleCustomerId()));
            record.setSellerTempKey(getOrCreateUUID("seller_" + record.getSaleSellerId()));
            record.setProductTempKey(getOrCreateUUID("product_" + record.getSaleProductId()));
            record.setStoreTempKey(getOrCreateUUID("store_" + (record.getStoreName() != null ? record.getStoreName() : record.getId())));
            record.setSupplierTempKey(getOrCreateUUID("supplier_" + (record.getSupplierName() != null ? record.getSupplierName() : record.getId())));
            record.setCategoryTempKey(getOrCreateUUID("category_" + (record.getProductCategory() != null ? record.getProductCategory() : record.getId())));
        } catch (Exception e) {
            log.error("Error setting keys for record: {}", record, e);
            return null;
        }
        return record;
    }

    private String getOrCreateUUID(String key) throws Exception {
        if (!keyState.contains(key)) {
            String uuid = UUID.randomUUID().toString();
            keyState.put(key, uuid);
            return uuid;
        }
        return keyState.get(key);
    }
}
