package com.example.model.contact_info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SupplierInfoDTO implements Serializable {
    private String supplierId;      // INT PRIMARY KEY, FK to dim_supplier
    private String supplierEmail;    // VARCHAR(50), UNIQUE
    private String supplierPhone;    // VARCHAR(50)
    private String supplierCountry;
    private String tempKey;// VARCHAR(50)
}
