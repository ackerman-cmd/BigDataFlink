package com.example.service;

import com.example.dto.CsvRecordDTO;
import com.example.model.contact_info.*;
import com.example.model.dim.*;
import com.example.model.fact.FactSalesDTO;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;

@Service
public class RecordMapperService implements Serializable {

    public DimCustomerDTO mapToCustomer(CsvRecordDTO record) {
        return DimCustomerDTO.builder()
                .customerId(record.getCustomerTempKey()) // UUID вместо saleCustomerId
                .firstName(record.getCustomerFirstName())
                .lastName(record.getCustomerLastName())
                .age(record.getCustomerAge())
                .tempKey(record.getCustomerTempKey())
                .build();
    }

    public CustomerContactInfoDTO mapToCustomerContactInfo(CsvRecordDTO record) {
        return CustomerContactInfoDTO.builder()
                .customerId(record.getCustomerTempKey())
                .customerEmail(record.getCustomerEmail())
                .customerCountry(record.getCustomerCountry())
                .customerPostalCode(record.getCustomerPostalCode())
                .tempKey(record.getCustomerTempKey())
                .build();
    }

    public CustomerPetInfoDTO mapToCustomerPetInfo(CsvRecordDTO record) {
        return CustomerPetInfoDTO.builder()
                .customerId(record.getCustomerTempKey())
                .petType(record.getCustomerPetType())
                .petName(record.getCustomerPetName())
                .petBreed(record.getCustomerPetBreed())
                .tempKey(record.getCustomerTempKey())
                .build();
    }

    public DimSellerDTO mapToSeller(CsvRecordDTO record) {
        return DimSellerDTO.builder()
                .sellerId(record.getSellerTempKey()) // UUID вместо saleSellerId
                .sellerFirstName(record.getSellerFirstName())
                .sellerLastName(record.getSellerLastName())
                .tempKey(record.getSellerTempKey())
                .build();
    }

    public SellerContactInfoDTO mapToSellerContactInfo(CsvRecordDTO record) {
        return SellerContactInfoDTO.builder()
                .sellerId(record.getSellerTempKey())
                .sellerEmail(record.getSellerEmail())
                .sellerCountry(record.getSellerCountry())
                .sellerPostalCode(record.getSellerPostalCode())
                .tempKey(record.getSellerTempKey())
                .build();
    }

    public DimProductDTO mapToProduct(CsvRecordDTO record) {
        return DimProductDTO.builder()
                .productId(record.getProductTempKey()) // UUID вместо saleProductId
                .productName(record.getProductName())
                .productCategory(record.getCategoryTempKey()) // UUID для product_categories
                .productPrice(record.getProductPrice().floatValue())
                .petCategory(record.getPetCategory())
                .productWeight(record.getProductWeight().floatValue())
                .productColor(record.getProductColor())
                .productSize(record.getProductSize())
                .productMaterial(record.getProductMaterial())
                .productBrand(record.getProductBrand())
                .productDescription(record.getProductDescription())
                .tempKey(record.getProductTempKey())
                .build();
    }

    public ProductStatisticsDTO mapToProductStatistics(CsvRecordDTO record) {
        return ProductStatisticsDTO.builder()
                .productId(record.getProductTempKey())
                .productRating(record.getProductRating().floatValue())
                .productReviews(record.getProductReviews())
                .productReleaseDate(record.getProductReleaseDate() != null ?
                        record.getProductReleaseDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null)
                .productExpiryDate(record.getProductExpiryDate() != null ?
                        record.getProductExpiryDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null)
                .tempKey(record.getProductTempKey())
                .build();
    }

    public DimStoreDTO mapToStore(CsvRecordDTO record) {
        return DimStoreDTO.builder()
                .storeId(record.getStoreTempKey()) // UUID вместо отсутствующего ID
                .storeName(record.getStoreName())
                .storeLocation(record.getStoreLocation())
                .storeCity(record.getStoreCity())
                .tempKey(record.getStoreTempKey())
                .build();
    }

    public StoreInfoDTO mapToStoreInfo(CsvRecordDTO record) {
        return StoreInfoDTO.builder()
                .storeId(record.getStoreTempKey())
                .storeState(record.getStoreState())
                .storeCountry(record.getStoreCountry())
                .storePhone(record.getStorePhone())
                .storeEmail(record.getStoreEmail())
                .tempKey(record.getStoreTempKey())
                .build();
    }

    public DimSupplierDTO mapToSupplier(CsvRecordDTO record) {
        return DimSupplierDTO.builder()
                .supplierId(record.getSupplierTempKey()) // UUID вместо id
                .supplierContact(record.getSupplierContact())
                .supplierCity(record.getSupplierCity())
                .supplierAddress(record.getSupplierAddress())
                .tempKey(record.getSupplierTempKey())
                .build();
    }

    public SupplierInfoDTO mapToSupplierInfo(CsvRecordDTO record) {
        return SupplierInfoDTO.builder()
                .supplierId(record.getSupplierTempKey())
                .supplierEmail(record.getSupplierEmail())
                .supplierPhone(record.getSupplierPhone())
                .supplierCountry(record.getSupplierCountry())
                .tempKey(record.getSupplierTempKey())
                .build();
    }

    public FactSalesDTO mapToFactSales(CsvRecordDTO record) {
        return FactSalesDTO.builder()
                .saleId(record.getTempKey())
                .customerTempKey(record.getCustomerTempKey())
                .productTempKey(record.getProductTempKey())
                .sellerTempKey(record.getSellerTempKey())
                .storeTempKey(record.getStoreTempKey())
                .supplierTempKey(record.getSupplierTempKey())
                .saleDate(record.getSaleDate() != null ?
                        record.getSaleDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null)
                .productQuantity(record.getSaleQuantity())
                .totalAmount(record.getSaleTotalPrice())
                .build();
    }

    public ProductCategoryDTO mapToProductCategory(CsvRecordDTO record) {
        return ProductCategoryDTO.builder()
                .categoryId(record.getCategoryTempKey())
                .categoryName(record.getProductCategory())
                .tempKey(record.getCategoryTempKey())
                .build();
    }
}