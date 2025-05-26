package com.example.model.fact;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FactSalesDTO implements Serializable {
    private Long saleId;            // SERIAL PRIMARY KEY
    private Long customerId;     // INT, FK to dim_customer
    private Long productId;      // INT, FK to dim_products
    private Long sellerId;       // INT, FK to dim_seller
    private Long storeId;        // INT, FK to dim_store
    private Long supplierId;     // INT, FK to dim_supplier
    private LocalDate saleDate;     // DATE
    private Integer productQuantity; // INT
    private Double totalAmount;  // DECIMAL
}
