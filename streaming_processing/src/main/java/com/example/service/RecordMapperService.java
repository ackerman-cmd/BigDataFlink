package com.example.service;

import com.example.dto.CsvRecordDTO;
import com.example.model.contact_info.*;
import com.example.model.dim.*;
import com.example.model.fact.FactSalesDTO;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

@Service
public class RecordMapperService implements Serializable {

    public DimCustomerDTO mapToCustomer(CsvRecordDTO record) {
        return DimCustomerDTO.builder()
                .customerId(record.getSaleCustomerId())
                .firstName(record.getCustomerFirstName())
                .lastName(record.getCustomerLastName())
                .age(record.getCustomerAge())
                .tempKey(record.getTempKey())
                .build();
    }

    public CustomerContactInfoDTO mapToCustomerContactInfo(CsvRecordDTO record) {
        return CustomerContactInfoDTO.builder()
                .customerId(record.getSaleCustomerId())
                .customerEmail(record.getCustomerEmail())
                .customerCountry(record.getCustomerCountry())
                .customerPostalCode(record.getCustomerPostalCode())
                .tempKey(record.getTempKey())
                .build();
    }

    public CustomerPetInfoDTO mapToCustomerPetInfo(CsvRecordDTO record) {
        return CustomerPetInfoDTO.builder()
                .customerId(record.getSaleCustomerId())
                .petType(record.getCustomerPetType())
                .petName(record.getCustomerPetName())
                .petBreed(record.getCustomerPetBreed())
                .tempKey(record.getTempKey())
                .build();
    }

    public DimSellerDTO mapToSeller(CsvRecordDTO record) {
        return DimSellerDTO.builder()
                .sellerId(record.getSaleSellerId())
                .sellerFirstName(record.getSellerFirstName())
                .sellerLastName(record.getSellerLastName())
                .tempKey(record.getTempKey())
                .build();
    }

    public SellerContactInfoDTO mapToSellerContactInfo(CsvRecordDTO record) {
        return SellerContactInfoDTO.builder()
                .sellerId(record.getSaleSellerId())
                .sellerEmail(record.getSellerEmail())
                .sellerCountry(record.getSellerCountry())
                .sellerPostalCode(record.getSellerPostalCode())
                .tempKey(record.getTempKey())
                .build();
    }

    public DimProductDTO mapToProduct(CsvRecordDTO record) {
        return DimProductDTO.builder()
                .productId(record.getSaleProductId())
                .productName(record.getProductName())
                .productCategory(Integer.parseInt(record.getProductCategory()))
                .productPrice(record.getProductPrice().floatValue())
                .tempKey(record.getTempKey())
                .build();
    }

    public ProductStatisticsDTO mapToProductStatistics(CsvRecordDTO record) {
        return ProductStatisticsDTO.builder()
                .productId(record.getSaleProductId())
                .productRating(record.getProductRating().floatValue())
                .productReviews(record.getProductReviews())
                .productReleaseDate(record.getProductReleaseDate()
                        .toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate())
                .productExpiryDate(record.getProductExpiryDate()
                        .toInstant()
                        .atZone(ZoneId.systemDefault())
                        .toLocalDate())
                .tempKey(record.getTempKey())
                .build();
    }

    public DimStoreDTO mapToStore(CsvRecordDTO record) {
        return DimStoreDTO.builder()
                .storeName(record.getStoreName())
                .storeLocation(record.getStoreLocation())
                .storeCity(record.getStoreCity())
                .tempKey(record.getTempKey())
                .build();
    }

    public StoreInfoDTO mapToStoreInfo(CsvRecordDTO record) {
        return StoreInfoDTO.builder()
                .storeId(record.getId())
                .storeState(record.getStoreState())
                .storeCountry(record.getStoreCountry())
                .storePhone(record.getStorePhone())
                .storeEmail(record.getStoreEmail())
                .tempKey(record.getTempKey())
                .build();

    }

    public DimSupplierDTO mapToSupplier(CsvRecordDTO record) {
        return DimSupplierDTO.builder()
                .supplierId(record.getId())
                .supplierContact(record.getSupplierContact())
                .supplierCity(record.getSupplierCity())
                .supplierAddress(record.getSupplierAddress())
                .tempKey(record.getTempKey())
                .build();

    }

    public SupplierInfoDTO mapToSupplierInfo(CsvRecordDTO record) {
        return SupplierInfoDTO.builder()
                .supplierId(record.getId())
                .supplierEmail(record.getSupplierEmail())
                .supplierPhone(record.getSupplierPhone())
                .supplierCountry(record.getSupplierCountry())
                .tempKey(record.getTempKey())
                .build();

    }

    public FactSalesDTO mapToFactSales(CsvRecordDTO record) {
        return FactSalesDTO.builder()
                .tempKey(record.getTempKey())
                .customerTempKey(record.getTempKey())
                .productTempKey(record.getTempKey())
                .sellerTempKey(record.getTempKey())
                .storeTempKey(record.getTempKey())
                .supplierTempKey(record.getTempKey())
                .saleDate(record.getSaleDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate())
                .productQuantity(record.getProductQuantity())
                .totalAmount(record.getSaleTotalPrice())
                .tempKey(record.getTempKey())
                .build();

    }


} 