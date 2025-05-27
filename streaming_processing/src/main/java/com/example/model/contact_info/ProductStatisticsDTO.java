package com.example.model.contact_info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.LocalDate;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ProductStatisticsDTO implements Serializable {
    private String productId;          // INT PRIMARY KEY, FK to dim_products
    private Float productRating;        // FLOAT
    private Integer productReviews;     // INT
    private LocalDate productReleaseDate; // DATE
    private LocalDate productExpiryDate;
    private String tempKey;// DATE
}
