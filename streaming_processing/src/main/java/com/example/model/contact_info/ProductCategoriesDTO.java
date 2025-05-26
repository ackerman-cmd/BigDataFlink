package com.example.model.contact_info;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class ProductCategoriesDTO implements Serializable {
    private Long categoryId;
    private String categoryName;
}
