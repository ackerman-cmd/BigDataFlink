package com.example.streaming;

import com.example.dto.CsvRecordDTO;
import com.example.service.RecordMapperService;
import com.example.util.JsonDeserializationFunction;
import com.example.util.LoggingSink;
import com.example.util.TempKeyMapper;
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

            DataStream<CsvRecordDTO> keyedRecordStream = recordStream
                    .map(new TempKeyMapper())
                    .returns(TypeInformation.of(CsvRecordDTO.class));

            // Создаем временные представления
            tableEnv.createTemporaryView("customer_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToCustomer).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("firstName", DataTypes.STRING())
                            .column("lastName", DataTypes.STRING())
                            .column("age", DataTypes.INT())
                            .build()
            ));
            tableEnv.createTemporaryView("customer_contact_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToCustomerContactInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("customerEmail", DataTypes.STRING())
                            .column("customerCountry", DataTypes.STRING())
                            .column("customerPostalCode", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("customer_pet_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToCustomerPetInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("petType", DataTypes.STRING())
                            .column("petName", DataTypes.STRING())
                            .column("petBreed", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("seller_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSeller).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("sellerFirstName", DataTypes.STRING())
                            .column("sellerLastName", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("seller_contact_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSellerContactInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("sellerEmail", DataTypes.STRING())
                            .column("sellerCountry", DataTypes.STRING())
                            .column("sellerPostalCode", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("product_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToProduct).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("productName", DataTypes.STRING())
                            .column("productPrice", DataTypes.FLOAT())
                            .column("productCategory", DataTypes.INT()) // Ошибка здесь!
                            .build()
            ));
            tableEnv.createTemporaryView("product_stats_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToProductStatistics).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("productRating", DataTypes.FLOAT())
                            .column("productReviews", DataTypes.INT())
                            .column("productReleaseDate", DataTypes.DATE())
                            .column("productExpiryDate", DataTypes.DATE())
                            .build()
            ));
            tableEnv.createTemporaryView("store_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToStore).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("storeName", DataTypes.STRING())
                            .column("storeLocation", DataTypes.STRING())
                            .column("storeCity", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("store_info_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToStoreInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("storeState", DataTypes.STRING())
                            .column("storeCountry", DataTypes.STRING())
                            .column("storePhone", DataTypes.STRING())
                            .column("storeEmail", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("supplier_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSupplier).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("supplierContact", DataTypes.STRING())
                            .column("supplierCity", DataTypes.STRING())
                            .column("supplierAddress", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("supplier_info_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToSupplierInfo).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
                            .column("supplierEmail", DataTypes.STRING())
                            .column("supplierPhone", DataTypes.STRING())
                            .column("supplierCountry", DataTypes.STRING())
                            .build()
            ));
            tableEnv.createTemporaryView("fact_sales_table", tableEnv.fromDataStream(
                    keyedRecordStream.map(recordMapperService::mapToFactSales).filter(dto -> dto != null),
                    Schema.newBuilder()
                            .column("tempKey", DataTypes.STRING())
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

            // Регистрация таблиц PostgreSQL для записи
            tableEnv.executeSql(
                    "CREATE TABLE dim_customer (" +
                            "customer_id STRING," +
                            "first_name STRING," +
                            "last_name STRING," +
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
                    "CREATE TABLE dim_seller (" +
                            "seller_id STRING," +
                            "seller_first_name STRING," +
                            "seller_last_name STRING," +
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
                    "CREATE TABLE dim_store (" +
                            "store_id STRING," +
                            "store_name STRING," +
                            "store_location STRING," +
                            "store_city STRING," +
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
                    "CREATE TABLE dim_supplier (" +
                            "supplier_id STRING," +
                            "supplier_contact STRING," +
                            "supplier_city STRING," +
                            "supplier_address STRING," +
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
                    "CREATE TABLE dim_products (" +
                            "product_id STRING," +
                            "product_name STRING," +
                            "product_price FLOAT," +
                            "product_category INT," +
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
                    "CREATE TABLE fact_sales (" +
                            "sale_id STRING," +
                            "customer_id STRING," +
                            "product_id STRING," +
                            "seller_id STRING," +
                            "store_id STRING," +
                            "supplier_id STRING," +
                            "sale_date DATE," +
                            "product_quantity INT," +
                            "total_amount DECIMAL(10,2)," +
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

            tableEnv.executeSql(
                    "CREATE TABLE error_log (" +
                            "error_log_id STRING," +
                            "temp_key STRING," +
                            "error_message STRING," +
                            "created_at TIMESTAMP," +
                            "PRIMARY KEY (error_log_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'error_log'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE customer_contact_info (" +
                            "customer_id STRING," +
                            "customer_email STRING," +
                            "customer_country STRING," +
                            "customer_postal_code STRING," +
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
                            "pet_type STRING," +
                            "pet_name STRING," +
                            "pet_breed STRING," +
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
                    "CREATE TABLE seller_contact_info (" +
                            "seller_id STRING," +
                            "seller_email STRING," +
                            "seller_country STRING," +
                            "seller_postal_code STRING," +
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
                    "CREATE TABLE store_info (" +
                            "store_id STRING," +
                            "store_state STRING," +
                            "store_country STRING," +
                            "store_phone STRING," +
                            "store_email STRING," +
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
                    "CREATE TABLE supplier_info (" +
                            "supplier_id STRING," +
                            "supplier_email STRING," +
                            "supplier_phone STRING," +
                            "supplier_country STRING," +
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
                    "CREATE TABLE id_mapping (" +
                            "temp_key STRING," +
                            "table_name STRING," +
                            "generated_id STRING," +
                            "created_at TIMESTAMP," +
                            "PRIMARY KEY (temp_key, table_name) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'id_mapping'," +
                            "'sink.buffer-flush.max-rows' = '1000'," +
                            "'sink.buffer-flush.interval' = '500ms'," +
                            "'sink.max-retries' = '3'" +
                            ")"
            );

            // Регистрация таблиц для чтения
            tableEnv.executeSql(
                    "CREATE TABLE dim_customer_read (" +
                            "customer_id STRING," +
                            "first_name STRING," +
                            "last_name STRING," +
                            "age INT," +
                            "PRIMARY KEY (customer_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_customer'," +
                            "'scan.fetch-size' = '1000'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_seller_read (" +
                            "seller_id STRING," +
                            "seller_first_name STRING," +
                            "seller_last_name STRING," +
                            "PRIMARY KEY (seller_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_seller'," +
                            "'scan.fetch-size' = '1000'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_store_read (" +
                            "store_id STRING," +
                            "store_name STRING," +
                            "store_location STRING," +
                            "store_city STRING," +
                            "PRIMARY KEY (store_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_store'," +
                            "'scan.fetch-size' = '1000'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_supplier_read (" +
                            "supplier_id STRING," +
                            "supplier_contact STRING," +
                            "supplier_city STRING," +
                            "supplier_address STRING," +
                            "PRIMARY KEY (supplier_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_supplier'," +
                            "'scan.fetch-size' = '1000'" +
                            ")"
            );

            tableEnv.executeSql(
                    "CREATE TABLE dim_products_read (" +
                            "product_id STRING," +
                            "product_name STRING," +
                            "product_price FLOAT," +
                            "product_category INT," +
                            "PRIMARY KEY (product_id) NOT ENFORCED" +
                            ") WITH (" +
                            "'connector' = 'jdbc'," +
                            "'url' = '" + JDBC_URL + "'," +
                            "'username' = '" + USERNAME + "'," +
                            "'password' = '" + PASSWORD + "'," +
                            "'table-name' = 'dim_products'," +
                            "'scan.fetch-size' = '1000'" +
                            ")"
            );

            // Вставка в справочники с идемпотентностью
            tableEnv.executeSql(
                    "INSERT INTO dim_customer (customer_id, first_name, last_name, age) " +
                            "SELECT c.tempKey, c.firstName, c.lastName, c.age " +
                            "FROM customer_table c " +
                            "LEFT JOIN dim_customer_read d " +
                            "    ON d.first_name = c.firstName AND d.last_name = c.lastName " +
                            "WHERE d.customer_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO id_mapping (temp_key, table_name, generated_id, created_at) " +
                            "SELECT c.tempKey, 'dim_customer', c.tempKey, CURRENT_TIMESTAMP " +
                            "FROM customer_table c " +
                            "JOIN dim_customer_read d " +
                            "    ON d.first_name = c.firstName AND d.last_name = c.lastName AND d.age = c.age"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_seller (seller_id, seller_first_name, seller_last_name) " +
                            "SELECT s.tempKey, s.sellerFirstName, s.sellerLastName " +
                            "FROM seller_table s " +
                            "LEFT JOIN dim_seller_read d " +
                            "    ON d.seller_first_name = s.sellerFirstName AND d.seller_last_name = s.sellerLastName " +
                            "WHERE d.seller_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO id_mapping (temp_key, table_name, generated_id, created_at) " +
                            "SELECT s.tempKey, 'dim_seller', s.tempKey, CURRENT_TIMESTAMP " +
                            "FROM seller_table s " +
                            "JOIN dim_seller_read d " +
                            "    ON d.seller_first_name = s.sellerFirstName AND d.seller_last_name = s.sellerLastName"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_store (store_id, store_name, store_location, store_city) " +
                            "SELECT s.tempKey, s.storeName, s.storeLocation, s.storeCity " +
                            "FROM store_table s " +
                            "LEFT JOIN dim_store_read d " +
                            "    ON d.store_name = s.storeName AND d.store_location = s.storeLocation " +
                            "WHERE d.store_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO id_mapping (temp_key, table_name, generated_id, created_at) " +
                            "SELECT s.tempKey, 'dim_store', s.tempKey, CURRENT_TIMESTAMP " +
                            "FROM store_table s " +
                            "JOIN dim_store_read d " +
                            "    ON d.store_name = s.storeName AND d.store_location = s.storeLocation AND d.store_city = s.storeCity"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_supplier (supplier_id, supplier_contact, supplier_city, supplier_address) " +
                            "SELECT s.tempKey, s.supplierContact, s.supplierCity, s.supplierAddress " +
                            "FROM supplier_table s " +
                            "LEFT JOIN dim_supplier_read d " +
                            "    ON d.supplier_contact = s.supplierContact AND d.supplier_address = s.supplierAddress " +
                            "WHERE d.supplier_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO id_mapping (temp_key, table_name, generated_id, created_at) " +
                            "SELECT s.tempKey, 'dim_supplier', s.tempKey, CURRENT_TIMESTAMP " +
                            "FROM supplier_table s " +
                            "JOIN dim_supplier_read d " +
                            "    ON d.supplier_contact = s.supplierContact AND d.supplier_city = s.supplierCity AND d.supplier_address = s.supplierAddress"
            );

            tableEnv.executeSql(
                    "INSERT INTO dim_products (product_id, product_name, product_price, product_category) " +
                            "SELECT p.tempKey, p.productName, p.productPrice, p.productCategory " +
                            "FROM product_table p " +
                            "LEFT JOIN dim_products_read d " +
                            "    ON d.product_name = p.productName AND d.product_category = p.productCategory " +
                            "WHERE d.product_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO id_mapping (temp_key, table_name, generated_id, created_at) " +
                            "SELECT p.tempKey, 'dim_products', p.tempKey, CURRENT_TIMESTAMP " +
                            "FROM product_table p " +
                            "JOIN dim_products_read d " +
                            "    ON d.product_name = p.productName AND d.product_price = p.productPrice AND d.product_category = p.productCategory"
            );

            // Вставка в связанные таблицы
            tableEnv.executeSql(
                    "INSERT INTO customer_contact_info (customer_id, customer_email, customer_country, customer_postal_code) " +
                            "SELECT m.generated_id, c.customerEmail, c.customerCountry, c.customerPostalCode " +
                            "FROM customer_contact_table c " +
                            "JOIN id_mapping m ON c.tempKey = m.temp_key AND m.table_name = 'dim_customer' " +
                            "LEFT JOIN customer_contact_info d ON d.customer_email = c.customerEmail " +
                            "WHERE d.customer_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO customer_pet_info (customer_id, pet_type, pet_name, pet_breed) " +
                            "SELECT m.generated_id, p.petType, p.petName, p.petBreed " +
                            "FROM customer_pet_table p " +
                            "JOIN id_mapping m ON p.tempKey = m.temp_key AND m.table_name = 'dim_customer' " +
                            "LEFT JOIN customer_pet_info d ON d.customer_id = m.generated_id " +
                            "WHERE d.customer_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO seller_contact_info (seller_id, seller_email, seller_country, seller_postal_code) " +
                            "SELECT m.generated_id, s.sellerEmail, s.sellerCountry, s.sellerPostalCode " +
                            "FROM seller_contact_table s " +
                            "JOIN id_mapping m ON s.tempKey = m.temp_key AND m.table_name = 'dim_seller' " +
                            "LEFT JOIN seller_contact_info d ON d.seller_email = s.sellerEmail " +
                            "WHERE d.seller_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO store_info (store_id, store_state, store_country, store_phone, store_email) " +
                            "SELECT m.generated_id, si.storeState, si.storeCountry, si.storePhone, si.storeEmail " +
                            "FROM store_info_table si " +
                            "JOIN id_mapping m ON si.tempKey = m.temp_key AND m.table_name = 'dim_store' " +
                            "LEFT JOIN store_info d ON d.store_email = si.storeEmail " +
                            "WHERE d.store_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO supplier_info (supplier_id, supplier_email, supplier_phone, supplier_country) " +
                            "SELECT m.generated_id, si.supplierEmail, si.supplierPhone, si.supplierCountry " +
                            "FROM supplier_info_table si " +
                            "JOIN id_mapping m ON si.tempKey = m.temp_key AND m.table_name = 'dim_supplier' " +
                            "LEFT JOIN supplier_info d ON d.supplier_email = si.supplierEmail " +
                            "WHERE d.supplier_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO product_statistics (product_id, product_rating, product_reviews, product_release_date, product_expiry_date) " +
                            "SELECT m.generated_id, ps.productRating, ps.productReviews, ps.productReleaseDate, ps.productExpiryDate " +
                            "FROM product_stats_table ps " +
                            "JOIN id_mapping m ON ps.tempKey = m.temp_key AND m.table_name = 'dim_products' " +
                            "LEFT JOIN product_statistics d ON d.product_id = m.generated_id " +
                            "WHERE d.product_id IS NULL"
            );

            // Вставка в fact_sales
            tableEnv.executeSql(
                    "INSERT INTO fact_sales (sale_id, customer_id, product_id, seller_id, store_id, supplier_id, sale_date, product_quantity, total_amount) " +
                            "SELECT " +
                            "    f.tempKey AS sale_id, " +
                            "    cm.generated_id AS customer_id, " +
                            "    pm.generated_id AS product_id, " +
                            "    sm.generated_id AS seller_id, " +
                            "    st.generated_id AS store_id, " +
                            "    spm.generated_id AS supplier_id, " +
                            "    f.saleDate, " +
                            "    f.productQuantity, " +
                            "    f.totalAmount " +
                            "FROM fact_sales_table f " +
                            "JOIN id_mapping cm ON f.customerTempKey = cm.temp_key AND cm.table_name = 'dim_customer' " +
                            "JOIN id_mapping pm ON f.productTempKey = pm.temp_key AND pm.table_name = 'dim_products' " +
                            "JOIN id_mapping sm ON f.sellerTempKey = sm.temp_key AND sm.table_name = 'dim_seller' " +
                            "JOIN id_mapping st ON f.storeTempKey = st.temp_key AND st.table_name = 'dim_store' " +
                            "JOIN id_mapping spm ON f.supplierTempKey = spm.temp_key AND spm.table_name = 'dim_supplier'"
            );

            // Логирование ошибок
            tableEnv.executeSql(
                    "INSERT INTO error_log (error_log_id, temp_key, error_message, created_at) " +
                            "SELECT UUID(), f.tempKey, 'Missing ID mapping', CURRENT_TIMESTAMP " +
                            "FROM fact_sales_table f " +
                            "LEFT JOIN id_mapping cm ON f.customerTempKey = cm.temp_key AND cm.table_name = 'dim_customer' " +
                            "WHERE cm.generated_id IS NULL"
            );

            tableEnv.executeSql(
                    "INSERT INTO error_log (error_log_id, temp_key, error_message, created_at) " +
                            "SELECT UUID(), c.tempKey, 'Duplicate customer record', CURRENT_TIMESTAMP " +
                            "FROM customer_table c " +
                            "JOIN dim_customer_read d ON c.firstName = d.first_name AND c.lastName = d.last_name " +
                            "WHERE d.customer_id IS NOT NULL"
            );

            // Запускаем выполнение
            env.execute(JOB_NAME);
        } catch (Exception e) {
            log.error("Error running Flink job", e);
            throw new RuntimeException("Failed to start Flink job", e);
        }
    }
}