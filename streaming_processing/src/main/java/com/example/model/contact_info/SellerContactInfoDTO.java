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
public class SellerContactInfoDTO implements Serializable {
    private String sellerId;         // INT PRIMARY KEY, FK to dim_seller
    private String sellerEmail;       // VARCHAR(50), UNIQUE
    private String sellerCountry;     // VARCHAR(50)
    private String sellerPostalCode;
    private String tempKey;// VARCHAR(50)
}