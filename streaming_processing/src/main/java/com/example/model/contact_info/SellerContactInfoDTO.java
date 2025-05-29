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
    private Long sellerId;
    private String sellerEmail;
    private String sellerCountry;
    private String sellerPostalCode;
}