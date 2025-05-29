package com.example.service;

import com.example.dto.CsvRecordDTO;
import com.example.model.contact_info.*;
import com.example.model.dim.DimCustomerDTO;
import com.example.model.dim.DimProductDTO;
import com.example.model.dim.DimSellerDTO;
import com.example.model.dim.DimStoreDTO;
import com.example.model.dim.DimSupplierDTO;
import com.example.model.fact.FactSalesDTO;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.time.ZoneId;

@Service
public class RecordMapperService implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(RecordMapperService.class);

    public DimCustomerDTO mapToCustomer(CsvRecordDTO record) {
        log.info("Маппинг в DimCustomerDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            DimCustomerDTO dto = DimCustomerDTO.builder()
                    .customerId(record.getId()) // 663
                    .firstName(record.getCustomerFirstName())
                    .lastName(record.getCustomerLastName())
                    .age(record.getCustomerAge())
                    .build();
            log.info("Успешно создан DimCustomerDTO: customerId={}, firstName={}", dto.getCustomerId(), dto.getFirstName());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в DimCustomerDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public DimSellerDTO mapToSeller(CsvRecordDTO record) {
        log.info("Маппинг в DimSellerDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            DimSellerDTO dto = DimSellerDTO.builder()
                    .sellerId(record.getId()) // 663
                    .sellerFirstName(record.getSellerFirstName())
                    .sellerLastName(record.getSellerLastName())
                    .build();
            log.info("Успешно создан DimSellerDTO: sellerId={}, sellerFirstName={}", dto.getSellerId(), dto.getSellerFirstName());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в DimSellerDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public DimStoreDTO mapToStore(CsvRecordDTO record) {
        log.info("Маппинг в DimStoreDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            DimStoreDTO dto = DimStoreDTO.builder()
                    .storeId(record.getId()) // 663
                    .storeName(record.getStoreName())
                    .storeLocation(record.getStoreLocation())
                    .storeCity(record.getStoreCity())
                    .build();
            log.info("Успешно создан DimStoreDTO: storeId={}, storeName={}", dto.getStoreId(), dto.getStoreName());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в DimStoreDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public DimSupplierDTO mapToSupplier(CsvRecordDTO record) {
        log.info("Маппинг в DimSupplierDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            DimSupplierDTO dto = DimSupplierDTO.builder()
                    .supplierId(record.getId()) // 663
                    .supplierContact(record.getSupplierContact())
                    .supplierCity(record.getSupplierCity())
                    .supplierAddress(record.getSupplierAddress())
                    .build();
            log.info("Успешно создан DimSupplierDTO: supplierId={}, supplierContact={}", dto.getSupplierId(), dto.getSupplierContact());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в DimSupplierDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public DimProductDTO mapToProduct(CsvRecordDTO record) {
        log.info("Маппинг в DimProductDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            DimProductDTO dto = DimProductDTO.builder()
                    .productId(record.getId()) // 663
                    .productName(record.getProductName())
                    .productPrice(record.getProductPrice() != null ? record.getProductPrice().floatValue() : 0.0f)
                    .productCategory(record.getProductCategory())
                    .petCategory(record.getPetCategory())
                    .productWeight(record.getProductWeight() != null ? record.getProductWeight().floatValue() : 0.0f)
                    .productColor(record.getProductColor())
                    .productSize(record.getProductSize())
                    .productMaterial(record.getProductMaterial())
                    .productBrand(record.getProductBrand())
                    .productDescription(record.getProductDescription())
                    .build();
            log.info("Успешно создан DimProductDTO: productId={}, productCategory={}", dto.getProductId(), dto.getProductCategory());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в DimProductDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public CustomerContactInfoDTO mapToCustomerContactInfo(CsvRecordDTO record) {
        log.info("Маппинг в CustomerContactInfoDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            CustomerContactInfoDTO dto = CustomerContactInfoDTO.builder()
                    .customerId(record.getId()) // 663
                    .customerEmail(record.getCustomerEmail())
                    .customerCountry(record.getCustomerCountry())
                    .customerPostalCode(record.getCustomerPostalCode())
                    .build();
            log.info("Успешно создан CustomerContactInfoDTO: customerId={}, customerEmail={}", dto.getCustomerId(), dto.getCustomerEmail());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в CustomerContactInfoDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public CustomerPetInfoDTO mapToCustomerPetInfo(CsvRecordDTO record) {
        log.info("Маппинг в CustomerPetInfoDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            CustomerPetInfoDTO dto = CustomerPetInfoDTO.builder()
                    .customerId(record.getId()) // 663
                    .petType(record.getCustomerPetType())
                    .petName(record.getCustomerPetName())
                    .petBreed(record.getCustomerPetBreed())
                    .build();
            log.info("Успешно создан CustomerPetInfoDTO: customerId={}, petType={}", dto.getCustomerId(), dto.getPetType());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в CustomerPetInfoDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public SellerContactInfoDTO mapToSellerContactInfo(CsvRecordDTO record) {
        log.info("Маппинг в SellerContactInfoDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            SellerContactInfoDTO dto = SellerContactInfoDTO.builder()
                    .sellerId(record.getId()) // 663
                    .sellerEmail(record.getSellerEmail())
                    .sellerCountry(record.getSellerCountry())
                    .sellerPostalCode(record.getSellerPostalCode())
                    .build();
            log.info("Успешно создан SellerContactInfoDTO: sellerId={}, sellerEmail={}", dto.getSellerId(), dto.getSellerEmail());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в SellerContactInfoDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public StoreInfoDTO mapToStoreInfo(CsvRecordDTO record) {
        log.info("Маппинг в StoreInfoDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            StoreInfoDTO dto = StoreInfoDTO.builder()
                    .storeId(record.getId()) // 663
                    .storeState(record.getStoreState())
                    .storeCountry(record.getStoreCountry())
                    .storePhone(record.getStorePhone())
                    .storeEmail(record.getStoreEmail())
                    .build();
            log.info("Успешно создан StoreInfoDTO: storeId={}, storeEmail={}", dto.getStoreId(), dto.getStoreEmail());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в StoreInfoDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    public SupplierInfoDTO mapToSupplierInfo(CsvRecordDTO record) {
        log.info("Маппинг в SupplierInfoDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            SupplierInfoDTO dto = SupplierInfoDTO.builder()
                    .supplierId(record.getId()) // 663
                    .supplierEmail(record.getSupplierEmail())
                    .supplierPhone(record.getSupplierPhone())
                    .supplierCountry(record.getSupplierCountry())
                    .build();
            log.info("Успешно создан SupplierInfoDTO: supplierId={}, supplierEmail={}", dto.getSupplierId(), dto.getSupplierEmail());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в SupplierInfoDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    // Обновлённый метод для product_statistics
    public ProductStatisticsDTO mapToProductStatistics(CsvRecordDTO record) {
        log.info("Маппинг в ProductStatisticsDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            log.info("Проверка полей: productRating={}, productReviews={}, productReleaseDate={}, productExpiryDate={}",
                    record.getProductRating(), record.getProductReviews(), record.getProductReleaseDate(), record.getProductExpiryDate());
            if (record.getProductRating() == null || record.getProductReviews() == null) {
                log.warn("Обнаружены null-поля: productRating={}, productReviews={}",
                        record.getProductRating(), record.getProductReviews());
            }
            ProductStatisticsDTO dto = ProductStatisticsDTO.builder()
                    .productId(record.getId()) // 663
                    .productRating(record.getProductRating() != null ? record.getProductRating().floatValue() : 0.0f)
                    .productReviews(record.getProductReviews() != null ? record.getProductReviews() : 0)
                    .productReleaseDate(record.getProductReleaseDate() != null ?
                            record.getProductReleaseDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null)
                    .productExpiryDate(record.getProductExpiryDate() != null ?
                            record.getProductExpiryDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null)
                    .build();
            log.info("Успешно создан ProductStatisticsDTO: productId={}, productRating={}, productReviews={}, releaseDate={}, expiryDate={}",
                    dto.getProductId(), dto.getProductRating(), dto.getProductReviews(),
                    dto.getProductReleaseDate(), dto.getProductExpiryDate());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в ProductStatisticsDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }

    // Обновлённый метод для fact_sales
    public FactSalesDTO mapToFactSales(CsvRecordDTO record) {
        log.info("Маппинг в FactSalesDTO: id={}", record != null ? record.getId() : null);
        if (record == null) {
            log.warn("record is null");
            return null;
        }
        try {
            log.info("Проверка полей: saleDate={}, saleQuantity={}, saleTotalPrice={}, saleCustomerId={}, saleProductId={}, saleSellerId={}",
                    record.getSaleDate(), record.getSaleQuantity(), record.getSaleTotalPrice(),
                    record.getSaleCustomerId(), record.getSaleProductId(), record.getSaleSellerId());
            if (record.getSaleDate() == null || record.getSaleQuantity() == null || record.getSaleTotalPrice() == null ||
                    record.getSaleCustomerId() == null || record.getSaleProductId() == null || record.getSaleSellerId() == null) {
                log.info("Обнаружены null-поля: saleDate={}, saleQuantity={}, saleTotalPrice={}, saleCustomerId={}, saleProductId={}, saleSellerId={}",
                        record.getSaleDate(), record.getSaleQuantity(), record.getSaleTotalPrice(),
                        record.getSaleCustomerId(), record.getSaleProductId(), record.getSaleSellerId());
            }
            FactSalesDTO dto = FactSalesDTO.builder()
                    .saleId(record.getId()) // 663
                    .customerId(record.getId()) // 77
                    .productId(record.getId()) // 77
                    .sellerId(record.getId()) // 77
                    .storeId(record.getId()) // 663
                    .supplierId(record.getId()) // 663
                    .saleDate(record.getSaleDate() != null ?
                            record.getSaleDate().toInstant().atZone(ZoneId.systemDefault()).toLocalDate() : null)
                    .productQuantity(record.getSaleQuantity() != null ? record.getSaleQuantity() : 0)
                    .totalAmount(record.getSaleTotalPrice() != null ? record.getSaleTotalPrice() : 0.0)
                    .build();
            log.info("Успешно создан FactSalesDTO: saleId={}, customerId={}, productId={}, sellerId={}, storeId={}, supplierId={}, saleDate={}",
                    dto.getSaleId(), dto.getCustomerId(), dto.getProductId(), dto.getSellerId(),
                    dto.getStoreId(), dto.getSupplierId(), dto.getSaleDate());
            return dto;
        } catch (Exception e) {
            log.error("Ошибка маппинга в FactSalesDTO: id={}. Ошибка: {}", record.getId(), e.getMessage(), e);
            return null;
        }
    }
}
