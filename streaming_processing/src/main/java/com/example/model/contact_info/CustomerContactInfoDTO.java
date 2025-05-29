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
    private Long customerId;
    private String customerEmail;
    private String customerCountry;
    private String customerPostalCode;

}