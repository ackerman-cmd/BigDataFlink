package com.example.validation;

import com.example.dto.CsvRecordDTO;

public class ValidationUtil {
    public static boolean isValidRecord(CsvRecordDTO dto) {
        if (dto == null) {
            return false;
        }
        return  dto.getCustomerAge() != null &&
                dto.getProductPrice() != null &&
                dto.getSaleDate() != null &&
                dto.getSaleCustomerId() != null &&
                dto.getSaleSellerId() != null &&
                dto.getSaleProductId() != null &&
                dto.getSaleQuantity() != null &&
                dto.getSaleTotalPrice() != null &&
                dto.getProductWeight() != null &&
                dto.getProductRating() != null &&
                dto.getProductReviews() != null;
    }
}
