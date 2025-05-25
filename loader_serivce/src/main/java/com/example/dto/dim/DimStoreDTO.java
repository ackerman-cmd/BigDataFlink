package com.example.dto.dim;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DimStoreDTO implements Serializable {
    private Long storeId;

    private String storeName;

    private String storeLocation;

    private String storeCity;
}
