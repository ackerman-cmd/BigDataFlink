package com.example.model.contact_info;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProductCategoryDTO {

     private String categoryId;
     private String categoryName;
     private String tempKey;

}
