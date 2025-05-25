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
public class StoreInfoDTO implements Serializable {
    private Long storeId;      // INT PRIMARY KEY, FK to dim_store
    private String storeState;    // VARCHAR(50)
    private String storeCountry;  // VARCHAR(50)
    private String storePhone;    // VARCHAR(50)
    private String storeEmail;    // VARCHAR(50), UNIQUE
}