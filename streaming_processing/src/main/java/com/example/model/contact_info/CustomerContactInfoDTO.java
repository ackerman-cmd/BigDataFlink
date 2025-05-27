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
public class CustomerContactInfoDTO implements Serializable {
    private Long customerId;       // INT PRIMARY KEY, FK to dim_customer
    private String customerEmail;     // VARCHAR(50), UNIQUE
    private String customerCountry;   // VARCHAR(50)
    private String customerPostalCode;
    private String tempKey;// VARCHAR(50)
}