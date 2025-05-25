package com.example.dto.contact_info;

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
    private Long supplierId;      // INT PRIMARY KEY, FK to dim_supplier
    private String supplierEmail;    // VARCHAR(50), UNIQUE
    private String supplierPhone;    // VARCHAR(50)
    private String supplierCountry;  // VARCHAR(50)
}
