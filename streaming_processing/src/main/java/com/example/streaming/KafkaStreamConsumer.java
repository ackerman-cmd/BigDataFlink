package com.example.streaming;

import com.example.dto.CsvRecordDTO;
import com.example.service.RecordMapperService;
import com.example.util.JsonDeserializationFunction;
import com.example.util.LoggingSink;
import com.example.util.TempKeyMapper;
import com.example.util.UniqueKeyMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.logging.log4j.Level;
import org.springframework.stereotype.Component;

import java.io.Serializable;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaStreamConsumer implements Serializable {
    private final KafkaSource<String> kafkaSource;
    private final RecordMapperService recordMapperService;
    private static final String JOB_NAME = "kafka-stream-consumer";

    // Параметры подключения к PostgreSQL
    private static final String JDBC_URL = "jdbc:postgresql://postgres:5432/sales_db";
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "secret";

    @PostConstruct
    public void start() {
        try {
            // Создаем окружение выполнения Flink
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(5);
            env.getConfig().disableForceKryo();
            env.getConfig().registerPojoType(CsvRecordDTO.class);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);


            // Создаем поток данных
            DataStream<String> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

            // Обрабатываем JSON и десериализуем в CsvRecordDTO
            DataStream<CsvRecordDTO> recordStream = stream
                    .process(new JsonDeserializationFunction())
                    .returns(TypeInformation.of(CsvRecordDTO.class));

// Логируем результаты
            recordStream.addSink(new LoggingSink());

// Добавляем uniqueKey для каждой записи
            DataStream<CsvRecordDTO> uniqueKeyStream = recordStream
                    .map(new UniqueKeyMapper())
                    .returns(TypeInformation.of(CsvRecordDTO.class));

// Добавляем tempKey и другие ключи с keyBy
            DataStream<CsvRecordDTO> keyedRecordStream = uniqueKeyStream
                    .keyBy(CsvRecordDTO::getUniqueKey) // Ключ по uniqueKey
                    .map(new TempKeyMapper())
                    .returns(TypeInformation.of(CsvRecordDTO.class));

            // Создаем временные представления на основе мапперов
            tableEnv.createTemporaryView("category_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToProductCategory).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("categoryId", DataTypes.STRING())
                            .column("categoryName", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("customer_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToCustomer).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("customerId", DataTypes.STRING())
                            .column("firstName", DataTypes.STRING())
                            .column("lastName", DataTypes.STRING())
                            .column("age", DataTypes.INT())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("customer_contact_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToCustomerContactInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("customerId", DataTypes.STRING())
                            .column("customerEmail", DataTypes.STRING())
                            .column("customerCountry", DataTypes.STRING())
                            .column("customerPostalCode", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("customer_pet_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToCustomerPetInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("customerId", DataTypes.STRING())
                            .column("petType", DataTypes.STRING())
                            .column("petName", DataTypes.STRING())
                            .column("petBreed", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("seller_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSeller).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("sellerId", DataTypes.STRING())
                            .column("sellerFirstName", DataTypes.STRING())
                            .column("sellerLastName", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("seller_contact_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSellerContactInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("sellerId", DataTypes.STRING())
                            .column("sellerEmail", DataTypes.STRING())
                            .column("sellerCountry", DataTypes.STRING())
                            .column("sellerPostalCode", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("product_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToProduct).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("productId", DataTypes.STRING())
                            .column("productName", DataTypes.STRING())
                            .column("productCategory", DataTypes.STRING())
                            .column("productPrice", DataTypes.FLOAT())
                            .column("petCategory", DataTypes.STRING())
                            .column("productWeight", DataTypes.FLOAT())
                            .column("productColor", DataTypes.STRING())
                            .column("productSize", DataTypes.STRING())
                            .column("productMaterial", DataTypes.STRING())
                            .column("productBrand", DataTypes.STRING())
                            .column("productDescription", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("product_stats_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToProductStatistics).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("productId", DataTypes.STRING())
                            .column("productRating", DataTypes.FLOAT())
                            .column("productReviews", DataTypes.INT())
                            .column("productReleaseDate", DataTypes.DATE())
                            .column("productExpiryDate", DataTypes.DATE())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("store_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToStore).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("storeId", DataTypes.STRING())
                            .column("storeName", DataTypes.STRING())
                            .column("storeLocation", DataTypes.STRING())
                            .column("storeCity", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("store_info_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToStoreInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("storeId", DataTypes.STRING())
                            .column("storeState", DataTypes.STRING())
                            .column("storeCountry", DataTypes.STRING())
                            .column("storePhone", DataTypes.STRING())
                            .column("storeEmail", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("supplier_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSupplier).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("supplierId", DataTypes.STRING())
                            .column("supplierContact", DataTypes.STRING())
                            .column("supplierCity", DataTypes.STRING())
                            .column("supplierAddress", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("supplier_info_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSupplierInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("supplierId", DataTypes.STRING())
                            .column("supplierEmail", DataTypes.STRING())
                            .column("supplierPhone", DataTypes.STRING())
                            .column("supplierCountry", DataTypes.STRING())
                            .column("tempKey", DataTypes.STRING())
                            .build()
            ));

            tableEnv.createTemporaryView("fact_sales_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToFactSales).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("saleId", DataTypes.STRING())
                            .column("customerTempKey", DataTypes.STRING())
                            .column("productTempKey", DataTypes.STRING())
                            .column("sellerTempKey", DataTypes.STRING())
                            .column("storeTempKey", DataTypes.STRING())
                            .column("supplierTempKey", DataTypes.STRING())
                            .column("saleDate", DataTypes.DATE())
                            .column("productQuantity", DataTypes.INT())
                            .column("totalAmount", DataTypes.DECIMAL(10, 2))
                            .build()
            ));

            // Определяем таблицы PostgreSQL
            tableEnv.executeSql(
                    "CREATE TABLE product_categories (" +
                            "category_id STRING," +
                            "category_name VARCHAR(50)," +
                            "PRIMARY KEY (category_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'product_categories'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_customer (" +
                            "customer_id STRING," +
                            "first_name VARCHAR(50)," +
                            "last_name VARCHAR(50)," +
                            "age INT," +
                            "PRIMARY KEY (customer_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_customer'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE customer_contact_info (" +
                            "customer_id STRING," +
                            "customer_email VARCHAR(50)," +
                            "customer_country VARCHAR(50)," +
                            "customer_postal_code VARCHAR(50)," +
                            "PRIMARY KEY (customer_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'customer_contact_info'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE customer_pet_info (" +
                            "customer_id STRING," +
                            "pet_type VARCHAR(50)," +
                            "pet_name VARCHAR(50)," +
                            "pet_breed VARCHAR(50)," +
                            "PRIMARY KEY (customer_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'customer_pet_info'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_seller (" +
                            "seller_id STRING," +
                            "seller_first_name VARCHAR(50)," +
                            "seller_last_name VARCHAR(50)," +
                            "PRIMARY KEY (seller_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_seller'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE seller_contact_info (" +
                            "seller_id STRING," +
                            "seller_email VARCHAR(50)," +
                            "seller_country VARCHAR(50)," +
                            "seller_postal_code VARCHAR(50)," +
                            "PRIMARY KEY (seller_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'seller_contact_info'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_products (" +
                            "product_id STRING," +
                            "product_name VARCHAR(50)," +
                            "product_price FLOAT," +
                            "product_category STRING," +
                            "pet_category VARCHAR(50)," +
                            "product_weight FLOAT," +
                            "product_color VARCHAR(50)," +
                            "product_size VARCHAR(50)," +
                            "product_material VARCHAR(50)," +
                            "product_brand VARCHAR(50)," +
                            "product_description STRING," +
                            "PRIMARY KEY (product_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_products'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE product_statistics (" +
                            "product_id STRING," +
                            "product_rating FLOAT," +
                            "product_reviews INT," +
                            "product_release_date DATE," +
                            "product_expiry_date DATE," +
                            "PRIMARY KEY (product_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'product_statistics'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_store (" +
                            "store_id STRING," +
                            "store_name VARCHAR(50)," +
                            "store_location VARCHAR(50)," +
                            "store_city VARCHAR(50)," +
                            "PRIMARY KEY (store_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_store'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE store_info (" +
                            "store_id STRING," +
                            "store_state VARCHAR(50)," +
                            "store_country VARCHAR(50)," +
                            "store_phone VARCHAR(50)," +
                            "store_email VARCHAR(50)," +
                            "PRIMARY KEY (store_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'store_info'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_supplier (" +
                            "supplier_id STRING," +
                            "supplier_contact VARCHAR(50)," +
                            "supplier_city VARCHAR(50)," +
                            "supplier_address VARCHAR(50)," +
                            "PRIMARY KEY (supplier_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_supplier'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE supplier_info (" +
                            "supplier_id STRING," +
                            "supplier_email VARCHAR(50)," +
                            "supplier_phone VARCHAR(50)," +
                            "supplier_country VARCHAR(50)," +
                            "PRIMARY KEY (supplier_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'supplier_info'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE fact_sales (" +
                            "sale_id STRING," +
                            "customer_id STRING," +
                            "product_id STRING," +
                            "seller_id STRING," +
                            "store_id STRING," +
                            "supplier_id STRING," +
                            "sale_date DATE," +
                            "product_quantity INT," +
                            "total_amount DECIMAL(10, 2)," +
                            "PRIMARY KEY (sale_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'fact_sales'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            // Вставки в таблицы
            tableEnv.executeSql(
                    "INSERT INTO product_categories (category_id, category_name) " +
                            "SELECT DISTINCT categoryId, categoryName " +
                            "FROM category_table " +
                            "WHERE categoryName IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_customer (customer_id, first_name, last_name, age) " +
                            "SELECT customerId, firstName, lastName, age " +
                            "FROM customer_table " +
                            "WHERE firstName IS NOT NULL AND lastName IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO customer_contact_info (customer_id, customer_email, customer_country, customer_postal_code) " +
                            "SELECT customerId, customerEmail, customerCountry, customerPostalCode " +
                            "FROM customer_contact_table " +
                            "WHERE customerEmail IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO customer_pet_info (customer_id, pet_type, pet_name, pet_breed) " +
                            "SELECT customerId, petType, petName, petBreed " +
                            "FROM customer_pet_table " +
                            "WHERE petType IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_seller (seller_id, seller_first_name, seller_last_name) " +
                            "SELECT sellerId, sellerFirstName, sellerLastName " +
                            "FROM seller_table " +
                            "WHERE sellerFirstName IS NOT NULL AND sellerLastName IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO seller_contact_info (seller_id, seller_email, seller_country, seller_postal_code) " +
                            "SELECT sellerId, sellerEmail, sellerCountry, sellerPostalCode " +
                            "FROM seller_contact_table " +
                            "WHERE sellerEmail IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_products (product_id, product_name, product_price, product_category, pet_category, product_weight, product_color, product_size, product_material, product_brand, product_description) " +
                            "SELECT " +
                            "    productId, " +
                            "    productName, " +
                            "    productPrice, " +
                            "    productCategory, " +
                            "    petCategory, " +
                            "    productWeight, " +
                            "    productColor, " +
                            "    productSize, " +
                            "    productMaterial, " +
                            "    productBrand, " +
                            "    productDescription " +
                            "FROM product_table " +
                            "WHERE productName IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO product_statistics (product_id, product_rating, product_reviews, product_release_date, product_expiry_date) " +
                            "SELECT productId, productRating, productReviews, productReleaseDate, productExpiryDate " +
                            "FROM product_stats_table " +
                            "WHERE productRating IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_store (store_id, store_name, store_location, store_city) " +
                            "SELECT storeId, storeName, storeLocation, storeCity " +
                            "FROM store_table " +
                            "WHERE storeName IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO store_info (store_id, store_state, store_country, store_phone, store_email) " +
                            "SELECT storeId, storeState, storeCountry, storePhone, storeEmail " +
                            "FROM store_info_table " +
                            "WHERE storeEmail IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_supplier (supplier_id, supplier_contact, supplier_city, supplier_address) " +
                            "SELECT supplierId, supplierContact, supplierCity, supplierAddress " +
                            "FROM supplier_table " +
                            "WHERE supplierContact IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO supplier_info (supplier_id, supplier_email, supplier_phone, supplier_country) " +
                            "SELECT supplierId, supplierEmail, supplierPhone, supplierCountry " +
                            "FROM supplier_info_table " +
                            "WHERE supplierEmail IS NOT NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO fact_sales (sale_id, customer_id, product_id, seller_id, store_id, supplier_id, sale_date, product_quantity, total_amount) " +
                            "SELECT " +
                            "    f.saleId, " +
                            "    f.customerTempKey, " +
                            "    f.productTempKey, " +
                            "    f.sellerTempKey, " +
                            "    f.storeTempKey, " +
                            "    f.supplierTempKey, " +
                            "    f.saleDate, " +
                            "    f.productQuantity, " +
                            "    f.totalAmount " +
                            "FROM fact_sales_table f " +
                            "LEFT JOIN customer_table c ON f.customerTempKey = c.customerId " +
                            "LEFT JOIN product_table p ON f.productTempKey = p.productId " +
                            "LEFT JOIN seller_table s ON f.sellerTempKey = s.sellerId " +
                            "LEFT JOIN store_table st ON f.storeTempKey = st.storeId " +
                            "LEFT JOIN supplier_table sp ON f.supplierTempKey = sp.supplierId " +
                            "WHERE f.saleDate IS NOT NULL " +
                            "AND c.customerId IS NOT NULL " +
                            "AND p.productId IS NOT NULL " +
                            "AND s.sellerId IS NOT NULL " +
                            "AND st.storeId IS NOT NULL " +
                            "AND sp.supplierId IS NOT NULL"
            );

            // Запускаем выполнение
            env.execute(JOB_NAME);
        } catch (Exception e) {
            log.error("Error running Flink job", e);
            throw new RuntimeException("Failed to start Flink job", e);
        }
    }
}