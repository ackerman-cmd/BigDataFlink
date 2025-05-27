package com.example.model.dim;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DimSupplierDTO implements Serializable {

    private Long supplierId;

    private String supplierContact;

    private String supplierCity;

    private String supplierAddress;
    private String tempKey;
}
