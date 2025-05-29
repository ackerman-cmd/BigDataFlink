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
    private Long supplierId;
    private String supplierEmail;
    private String supplierPhone;
    private String supplierCountry;
}
