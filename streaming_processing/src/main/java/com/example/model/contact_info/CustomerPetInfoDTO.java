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
public class CustomerPetInfoDTO implements Serializable {
    private Long customerId;
    private String petType;
    private String petName;
    private String petBreed;

}
