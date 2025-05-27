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
public class CustomerPetInfoDTO implements Serializable {
    private Long customerId;    // INT PRIMARY KEY, FK to dim_customer
    private String petType;       // VARCHAR(50)
    private String petName;       // VARCHAR(50)
    private String petBreed;
    private String tempKey;// VARCHAR(50)
}
