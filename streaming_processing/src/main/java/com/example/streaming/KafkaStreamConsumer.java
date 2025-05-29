package com.example.streaming;

import com.example.dto.CsvRecordDTO;
import com.example.service.RecordMapperService;
import com.example.util.JsonDeserializationFunction;
import com.example.util.LoggingSink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.io.Serializable;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamConsumer implements Serializable {
    private final KafkaSource<String> kafkaSource;
    private final RecordMapperService recordMapperService;
    private static final String JOB_NAME = "kafka-stream-consumer";

    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/sales_db";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "secret";

    @PostConstruct
    public void start() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.getConfig().disableForceKryo();
            // Отключение Kryo для Arrays$ArrayList
            // Регистрация кастомного сериализатора для Arrays$ArrayList
            env.getConfig().addDefaultKryoSerializer(
                    Arrays.asList().getClass(),
                    ArraysAsListSerializer.class
            );

            DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            DataStream<CsvRecordDTO> recordStream = stream
                    .process(new JsonDeserializationFunction())
                    .returns(TypeInformation.of(CsvRecordDTO.class));

            recordStream.addSink(new LoggingSink());

            JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                    .withBatchSize(1)
                    .withBatchIntervalMs(0)
                    .withMaxRetries(3)
                    .build();

            JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(JDBC_URL)
                    .withUsername(USERNAME)
                    .withPassword(PASSWORD)
                    .build();

            // 1. Sink для dim_customer
            recordStream
                    .map(recordMapperService::mapToCustomer)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null DimCustomerDTO");
                            return false;
                        }
                        log.info("Отправка в dim_customer: customerId={}, firstName={}",
                                dto.getCustomerId(), dto.getFirstName());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO dim_customer (customer_id, first_name, last_name, age) VALUES (?, ?, ?, ?) ON CONFLICT (customer_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getCustomerId());
                                    ps.setString(2, dto.getFirstName());
                                    ps.setString(3, dto.getLastName());
                                    ps.setInt(4, dto.getAge());
                                    log.info("Подготовлен INSERT в dim_customer: customerId={}", dto.getCustomerId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в dim_customer: customerId={}, ошибка={}",
                                            dto.getCustomerId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 2. Sink для dim_seller
            recordStream
                    .map(recordMapperService::mapToSeller)
                    .filter(dto -> {
                        if (dto == null) {
                            log.info("Отфильтрован null DimSellerDTO");
                            return false;
                        }
                        log.info("Отправка в dim_seller: sellerId={}, sellerFirstName={}",
                                dto.getSellerId(), dto.getSellerFirstName());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO dim_seller (seller_id, seller_first_name, seller_last_name) VALUES (?, ?, ?) ON CONFLICT (seller_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getSellerId());
                                    ps.setString(2, dto.getSellerFirstName());
                                    ps.setString(3, dto.getSellerLastName());
                                    log.info("Подготовлен INSERT в dim_seller: sellerId={}", dto.getSellerId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в dim_seller: sellerId={}, ошибка={}",
                                            dto.getSellerId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 3. Sink для dim_store
            recordStream
                    .map(recordMapperService::mapToStore)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null DimStoreDTO");
                            return false;
                        }
                        log.info("Отправка в dim_store: storeId={}, storeName={}",
                                dto.getStoreId(), dto.getStoreName());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO dim_store (store_id, store_name, store_location, store_city) VALUES (?, ?, ?, ?) ON CONFLICT (store_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getStoreId());
                                    ps.setString(2, dto.getStoreName());
                                    ps.setString(3, dto.getStoreLocation());
                                    ps.setString(4, dto.getStoreCity());
                                    log.info("Подготовлен INSERT в dim_store: storeId={}", dto.getStoreId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в dim_store: storeId={}, ошибка={}",
                                            dto.getStoreId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 4. Sink для dim_supplier
            recordStream
                    .map(recordMapperService::mapToSupplier)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null DimSupplierDTO");
                            return false;
                        }
                        log.info("Отправка в dim_supplier: supplierId={}, supplierContact={}",
                                dto.getSupplierId(), dto.getSupplierContact());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO dim_supplier (supplier_id, supplier_contact, supplier_city, supplier_address) VALUES (?, ?, ?, ?) ON CONFLICT (supplier_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getSupplierId());
                                    ps.setString(2, dto.getSupplierContact());
                                    ps.setString(3, dto.getSupplierCity());
                                    ps.setString(4, dto.getSupplierAddress());
                                    log.info("Подготовлен INSERT в dim_supplier: supplierId={}", dto.getSupplierId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в dim_supplier: supplierId={}, ошибка={}",
                                            dto.getSupplierId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 5. Sink для dim_products
            recordStream
                    .map(recordMapperService::mapToProduct)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null DimProductDTO");
                            return false;
                        }
                        log.info("Отправка в dim_products: productId={}, productCategory={}",
                                dto.getProductId(), dto.getProductCategory());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO dim_products (product_id, product_name, product_price, product_category, pet_category, product_weight, product_color, product_size, product_material, product_brand, product_description) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (product_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getProductId());
                                    ps.setString(2, dto.getProductName());
                                    ps.setFloat(3, dto.getProductPrice());
                                    ps.setString(4, dto.getProductCategory());
                                    ps.setString(5, dto.getPetCategory());
                                    ps.setFloat(6, dto.getProductWeight());
                                    ps.setString(7, dto.getProductColor());
                                    ps.setString(8, dto.getProductSize());
                                    ps.setString(9, dto.getProductMaterial());
                                    ps.setString(10, dto.getProductBrand());
                                    ps.setString(11, dto.getProductDescription());
                                    log.info("Подготовлен INSERT в dim_products: productId={}", dto.getProductId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в dim_products: productId={}, ошибка={}",
                                            dto.getProductId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 6. Sink для customer_contact_info
            recordStream
                    .map(recordMapperService::mapToCustomerContactInfo)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null CustomerContactInfoDTO");
                            return false;
                        }
                        log.info("Отправка в customer_contact_info: customerId={}, customerEmail={}",
                                dto.getCustomerId(), dto.getCustomerEmail());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO customer_contact_info (customer_id, customer_email, customer_country, customer_postal_code) VALUES (?, ?, ?, ?) ON CONFLICT (customer_email) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getCustomerId());
                                    ps.setString(2, dto.getCustomerEmail());
                                    ps.setString(3, dto.getCustomerCountry());
                                    ps.setString(4, dto.getCustomerPostalCode());
                                    log.info("Подготовлен INSERT в customer_contact_info: customerId={}", dto.getCustomerId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в customer_contact_info: customerId={}, customerEmail={}, ошибка={}",
                                            dto.getCustomerId(), dto.getCustomerEmail(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 7. Sink для customer_pet_info
            recordStream
                    .map(recordMapperService::mapToCustomerPetInfo)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null CustomerPetInfoDTO");
                            return false;
                        }
                        log.info("Отправка в customer_pet_info: customerId={}, petType={}",
                                dto.getCustomerId(), dto.getPetType());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO customer_pet_info (customer_id, pet_type, pet_name, pet_breed) VALUES (?, ?, ?, ?) ON CONFLICT (customer_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getCustomerId());
                                    ps.setString(2, dto.getPetType());
                                    ps.setString(3, dto.getPetName());
                                    ps.setString(4, dto.getPetBreed());
                                    log.info("Подготовлен INSERT в customer_pet_info: customerId={}", dto.getCustomerId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в customer_pet_info: customerId={}, ошибка={}",
                                            dto.getCustomerId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 8. Sink для seller_contact_info
            recordStream
                    .map(recordMapperService::mapToSellerContactInfo)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null SellerContactInfoDTO");
                            return false;
                        }
                        log.info("Отправка в seller_contact_info: sellerId={}, sellerEmail={}",
                                dto.getSellerId(), dto.getSellerEmail());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO seller_contact_info (seller_id, seller_email, seller_country, seller_postal_code) VALUES (?, ?, ?, ?) ON CONFLICT (seller_email) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getSellerId());
                                    ps.setString(2, dto.getSellerEmail());
                                    ps.setString(3, dto.getSellerCountry());
                                    ps.setString(4, dto.getSellerPostalCode());
                                    log.info("Подготовлен INSERT в seller_contact_info: sellerId={}", dto.getSellerId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в seller_contact_info: sellerId={}, sellerEmail={}, ошибка={}",
                                            dto.getSellerId(), dto.getSellerEmail(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 9. Sink для store_info
            recordStream
                    .map(recordMapperService::mapToStoreInfo)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null StoreInfoDTO");
                            return false;
                        }
                        log.info("Отправка в store_info: storeId={}, storeEmail={}",
                                dto.getStoreId(), dto.getStoreEmail());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO store_info (store_id, store_state, store_country, store_phone, store_email) VALUES (?, ?, ?, ?, ?) ON CONFLICT (store_email) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getStoreId());
                                    ps.setString(2, dto.getStoreState());
                                    ps.setString(3, dto.getStoreCountry());
                                    ps.setString(4, dto.getStorePhone());
                                    ps.setString(5, dto.getStoreEmail());
                                    log.info("Подготовлен INSERT в store_info: storeId={}", dto.getStoreId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в store_info: storeId={}, storeEmail={}, ошибка={}",
                                            dto.getStoreId(), dto.getStoreEmail(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // 10. Sink для supplier_info
            recordStream
                    .map(recordMapperService::mapToSupplierInfo)
                    .filter(dto -> {
                        if (dto == null) {
                            log.warn("Отфильтрован null SupplierInfoDTO");
                            return false;
                        }
                        log.info("Отправка в supplier_info: supplierId={}, supplierEmail={}",
                                dto.getSupplierId(), dto.getSupplierEmail());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO supplier_info (supplier_id, supplier_email, supplier_phone, supplier_country) VALUES (?, ?, ?, ?) ON CONFLICT (supplier_email) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    ps.setLong(1, dto.getSupplierId());
                                    ps.setString(2, dto.getSupplierEmail());
                                    ps.setString(3, dto.getSupplierPhone());
                                    ps.setString(4, dto.getSupplierCountry());
                                    log.info("Подготовлен INSERT в supplier_info: supplierId={}", dto.getSupplierId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в supplier_info: supplierId={}, supplierEmail={}, ошибка={}",
                                            dto.getSupplierId(), dto.getSupplierEmail(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // Обновлённый Sink для product_statistics
            recordStream
                    .map(recordMapperService::mapToProductStatistics)
                    .filter(dto -> {
                        if (dto == null) {
                            log.info("Отфильтрован null ProductStatisticsDTO");
                            return false;
                        }
                        if (dto.getProductId() == null || dto.getProductRating() == null || dto.getProductReviews() == null) {
                            log.info("Недопустимые данные в ProductStatisticsDTO: productId={}, productRating={}, productReviews={}",
                                    dto.getProductId(), dto.getProductRating(), dto.getProductReviews());
                            return false;
                        }
                        log.info("Отправка в product_statistics: productId={}, productRating={}, productReviews={}, releaseDate={}, expiryDate={}",
                                dto.getProductId(), dto.getProductRating(), dto.getProductReviews(),
                                dto.getProductReleaseDate(), dto.getProductExpiryDate());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO product_statistics (product_id, product_rating, product_reviews, product_release_date, product_expiry_date) VALUES (?, ?, ?, ?, ?) ON CONFLICT (product_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    log.info("Подготовка INSERT в product_statistics: productId={}, productRating={}, productReviews={}, releaseDate={}, expiryDate={}",
                                            dto.getProductId(), dto.getProductRating(), dto.getProductReviews(),
                                            dto.getProductReleaseDate(), dto.getProductExpiryDate());
                                    ps.setLong(1, dto.getProductId());
                                    ps.setFloat(2, dto.getProductRating());
                                    ps.setInt(3, dto.getProductReviews());
                                    ps.setDate(4, dto.getProductReleaseDate() != null ? Date.valueOf(dto.getProductReleaseDate()) : null);
                                    ps.setDate(5, dto.getProductExpiryDate() != null ? Date.valueOf(dto.getProductExpiryDate()) : null);
                                    log.info("Подготовлен INSERT в product_statistics: productId={}", dto.getProductId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в product_statistics: productId={}, ошибка={}",
                                            dto.getProductId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            // Обновлённый Sink для fact_sales
            recordStream
                    .map(recordMapperService::mapToFactSales)
                    .filter(dto -> {
                        if (dto == null) {
                            log.info("Отфильтрован null FactSalesDTO");
                            return false;
                        }
                        if (dto.getSaleId() == null || dto.getCustomerId() == null || dto.getProductId() == null ||
                                dto.getSellerId() == null || dto.getSaleDate() == null) {
                            log.info("Недопустимые данные в FactSalesDTO: saleId={}, customerId={}, productId={}, sellerId={}, saleDate={}",
                                    dto.getSaleId(), dto.getCustomerId(), dto.getProductId(), dto.getSellerId(), dto.getSaleDate());
                            return false;
                        }
                        log.info("Отправка в fact_sales: saleId={}, customerId={}, productId={}, sellerId={}, saleDate={}",
                                dto.getSaleId(), dto.getCustomerId(), dto.getProductId(), dto.getSellerId(), dto.getSaleDate());
                        return true;
                    })
                    .addSink(JdbcSink.sink(
                            "INSERT INTO fact_sales (sale_id, customer_id, product_id, seller_id, store_id, supplier_id, sale_date, product_quantity, total_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (sale_id) DO NOTHING",
                            (ps, dto) -> {
                                try {
                                    log.info("Подготовка INSERT в fact_sales: saleId={}, customerId={}, productId={}, sellerId={}, saleDate={}",
                                            dto.getSaleId(), dto.getCustomerId(), dto.getProductId(), dto.getSellerId(), dto.getSaleDate());
                                    ps.setLong(1, dto.getSaleId());
                                    ps.setLong(2, dto.getCustomerId());
                                    ps.setLong(3, dto.getProductId());
                                    ps.setLong(4, dto.getSellerId());
                                    ps.setLong(5, dto.getStoreId());
                                    ps.setLong(6, dto.getSupplierId());
                                    ps.setDate(7, Date.valueOf(dto.getSaleDate()));
                                    ps.setInt(8, dto.getProductQuantity());
                                    ps.setDouble(9, dto.getTotalAmount());
                                    log.info("Подготовлен INSERT в fact_sales: saleId={}, customerId={}",
                                            dto.getSaleId(), dto.getCustomerId());
                                } catch (SQLException e) {
                                    log.info("Ошибка подготовки INSERT в fact_sales: saleId={}, customerId={}, ошибка={}",
                                            dto.getSaleId(), dto.getCustomerId(), e.getMessage(), e);
                                    throw e;
                                }
                            },
                            executionOptions,
                            connectionOptions
                    ));

            env.execute(JOB_NAME);
        } catch (Exception e) {
            log.error("Ошибка выполнения Flink-задания: {}", e.getMessage(), e);
            throw new RuntimeException("Не удалось запустить Flink-задание", e);
        }
    }
}
