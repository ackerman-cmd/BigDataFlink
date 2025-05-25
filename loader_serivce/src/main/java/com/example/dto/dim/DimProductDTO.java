package com.example.dto.dim;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DimProductDTO implements Serializable {
    private Long productId;
    private String productName;
    private Float productPrice;
    private Integer productCategory;
    private String petCategory;
    private Float productWeight;
    private String productColor;
    private String productSize;
    private String productMaterial;
    private String productBrand;
    private String productDescription;
}
