package com.example.model.fact;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import java.io.Serializable;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FactSalesDTO implements Serializable {
    private String saleId;            // SERIAL PRIMARY KEY
    private String customerTempKey;     // INT, FK to dim_customer
    private String productTempKey;      // INT, FK to dim_products
    private String sellerTempKey;       // INT, FK to dim_seller
    private String storeTempKey;        // INT, FK to dim_store
    private String supplierTempKey;     // INT, FK to dim_supplier
    private LocalDate saleDate;     // DATE
    private Integer productQuantity; // INT
    private Double totalAmount;  // DECIMAL



}
