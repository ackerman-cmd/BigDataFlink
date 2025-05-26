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
public class DimSellerDTO implements Serializable {
    private Long sellerId;

    private String sellerFirstName;

    private String sellerLastName;

}
