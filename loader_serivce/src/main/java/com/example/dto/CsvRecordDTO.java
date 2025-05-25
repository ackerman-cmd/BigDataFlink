package com.example.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.opencsv.bean.CsvBindByName;
import com.opencsv.bean.CsvDate;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CsvRecordDTO {
    @JsonProperty("id")
    @CsvBindByName(column = "id")
    private Long id;

    @JsonProperty("customer_first_name")
    @CsvBindByName(column = "customer_first_name")
    private String customerFirstName;

    @JsonProperty("customer_last_name")
    @CsvBindByName(column = "customer_last_name")
    private String customerLastName;

    @JsonProperty("customer_age")
    @CsvBindByName(column = "customer_age")
    private Integer customerAge;

    @JsonProperty("customer_email")
    @CsvBindByName(column = "customer_email")
    private String customerEmail;

    @JsonProperty("customer_country")
    @CsvBindByName(column = "customer_country")
    private String customerCountry;

    @JsonProperty("customer_postal_code")
    @CsvBindByName(column = "customer_postal_code")
    private String customerPostalCode;

    @JsonProperty("customer_pet_type")
    @CsvBindByName(column = "customer_pet_type")
    private String customerPetType;

    @JsonProperty("customer_pet_name")
    @CsvBindByName(column = "customer_pet_name")
    private String customerPetName;

    @JsonProperty("customer_pet_breed")
    @CsvBindByName(column = "customer_pet_breed")
    private String customerPetBreed;

    @JsonProperty("seller_first_name")
    @CsvBindByName(column = "seller_first_name")
    private String sellerFirstName;

    @JsonProperty("seller_last_name")
    @CsvBindByName(column = "seller_last_name")
    private String sellerLastName;

    @JsonProperty("seller_email")
    @CsvBindByName(column = "seller_email")
    private String sellerEmail;

    @JsonProperty("seller_country")
    @CsvBindByName(column = "seller_country")
    private String sellerCountry;

    @JsonProperty("seller_postal_code")
    @CsvBindByName(column = "seller_postal_code")
    private String sellerPostalCode;

    @JsonProperty("product_name")
    @CsvBindByName(column = "product_name")
    private String productName;

    @JsonProperty("product_category")
    @CsvBindByName(column = "product_category")
    private String productCategory;

    @JsonProperty("product_price")
    @CsvBindByName(column = "product_price")
    private Double productPrice;

    @JsonProperty("product_quantity")
    @CsvBindByName(column = "product_quantity")
    private Integer productQuantity;

    @JsonProperty("sale_date")
    @JsonFormat(pattern = "M/d/yyyy")
    @CsvBindByName(column = "sale_date")
    @CsvDate("M/d/yyyy")
    private LocalDate saleDate;

    @JsonProperty("sale_customer_id")
    @CsvBindByName(column = "sale_customer_id")
    private Long saleCustomerId;

    @JsonProperty("sale_seller_id")
    @CsvBindByName(column = "sale_seller_id")
    private Long saleSellerId;

    @JsonProperty("sale_product_id")
    @CsvBindByName(column = "sale_product_id")
    private Long saleProductId;

    @JsonProperty("sale_quantity")
    @CsvBindByName(column = "sale_quantity")
    private Integer saleQuantity;

    @JsonProperty("sale_total_price")
    @CsvBindByName(column = "sale_total_price")
    private Double saleTotalPrice;

    @JsonProperty("store_name")
    @CsvBindByName(column = "store_name")
    private String storeName;

    @JsonProperty("store_location")
    @CsvBindByName(column = "store_location")
    private String storeLocation;

    @JsonProperty("store_city")
    @CsvBindByName(column = "store_city")
    private String storeCity;

    @JsonProperty("store_state")
    @CsvBindByName(column = "store_state")
    private String storeState;

    @JsonProperty("store_country")
    @CsvBindByName(column = "store_country")
    private String storeCountry;

    @JsonProperty("store_phone")
    @CsvBindByName(column = "store_phone")
    private String storePhone;

    @JsonProperty("store_email")
    @CsvBindByName(column = "store_email")
    private String storeEmail;

    @JsonProperty("pet_category")
    @CsvBindByName(column = "pet_category")
    private String petCategory;

    @JsonProperty("product_weight")
    @CsvBindByName(column = "product_weight")
    private Double productWeight;

    @JsonProperty("product_color")
    @CsvBindByName(column = "product_color")
    private String productColor;

    @JsonProperty("product_size")
    @CsvBindByName(column = "product_size")
    private String productSize;

    @JsonProperty("product_brand")
    @CsvBindByName(column = "product_brand")
    private String productBrand;

    @JsonProperty("product_material")
    @CsvBindByName(column = "product_material")
    private String productMaterial;

    @JsonProperty("product_description")
    @CsvBindByName(column = "product_description")
    private String productDescription;

    @JsonProperty("product_rating")
    @CsvBindByName(column = "product_rating")
    private Double productRating;

    @JsonProperty("product_reviews")
    @CsvBindByName(column = "product_reviews")
    private Integer productReviews;

    @JsonProperty("product_release_date")
    @JsonFormat(pattern = "M/d/yyyy")
    @CsvBindByName(column = "product_release_date")
    @CsvDate("M/d/yyyy")
    private LocalDate productReleaseDate;

    @JsonProperty("product_expiry_date")
    @JsonFormat(pattern = "M/d/yyyy")
    @CsvBindByName(column = "product_expiry_date")
    @CsvDate("M/d/yyyy")
    private LocalDate productExpiryDate;

    @JsonProperty("supplier_name")
    @CsvBindByName(column = "supplier_name")
    private String supplierName;

    @JsonProperty("supplier_contact")
    @CsvBindByName(column = "supplier_contact")
    private String supplierContact;

    @JsonProperty("supplier_email")
    @CsvBindByName(column = "supplier_email")
    private String supplierEmail;

    @JsonProperty("supplier_phone")
    @CsvBindByName(column = "supplier_phone")
    private String supplierPhone;

    @JsonProperty("supplier_address")
    @CsvBindByName(column = "supplier_address")
    private String supplierAddress;

    @JsonProperty("supplier_city")
    @CsvBindByName(column = "supplier_city")
    private String supplierCity;

    @JsonProperty("supplier_country")
    @CsvBindByName(column = "supplier_country")
    private String supplierCountry;
}