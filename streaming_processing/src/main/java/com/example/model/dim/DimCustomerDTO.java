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
public class DimCustomerDTO implements Serializable {
    private Long customerId;

    private String firstName;

    private String lastName;

    private int age;

}
