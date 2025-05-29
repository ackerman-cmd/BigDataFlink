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
public class StoreInfoDTO implements Serializable {
    private Long storeId;
    private String storeState;
    private String storeCountry;
    private String storePhone;
    private String storeEmail;
}