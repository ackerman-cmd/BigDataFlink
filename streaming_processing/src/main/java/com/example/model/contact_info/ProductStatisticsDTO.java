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
    private Long productId;
    private Float productRating;
    private Integer productReviews;
    private LocalDate productReleaseDate;
    private LocalDate productExpiryDate;

}
