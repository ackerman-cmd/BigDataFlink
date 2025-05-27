-- Удаление таблиц в правильном порядке (от зависимых к независимым)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS product_statistics;
DROP TABLE IF EXISTS supplier_info;
DROP TABLE IF EXISTS store_info;
DROP TABLE IF EXISTS seller_contact_info;
DROP TABLE IF EXISTS customer_contact_info;
DROP TABLE IF EXISTS customer_pet_info;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS product_categories;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_customer;

-- Создание таблиц в правильном порядке (от независимых к зависимым)

-- 1. Клиенты
CREATE TABLE dim_customer (
                              customer_id TEXT PRIMARY KEY,
                              first_name VARCHAR(50),
                              last_name VARCHAR(50),
                              age INT
);

-- 2. Продавцы
CREATE TABLE dim_seller (
                            seller_id TEXT PRIMARY KEY,
                            seller_first_name VARCHAR(50),
                            seller_last_name VARCHAR(50)
);

-- 3. Категории товаров
CREATE TABLE product_categories (
                                    category_id TEXT PRIMARY KEY,
                                    category_name VARCHAR(50)
);

-- 4. Магазины
CREATE TABLE dim_store (
                           store_id TEXT PRIMARY KEY,
                           store_name VARCHAR(50),
                           store_location VARCHAR(50),
                           store_city VARCHAR(50)
);

-- 5. Поставщики
CREATE TABLE dim_supplier (
                              supplier_id TEXT PRIMARY KEY,
                              supplier_contact VARCHAR(50),
                              supplier_city VARCHAR(50),
                              supplier_address VARCHAR(50)
);

-- 6. Товары
CREATE TABLE dim_products (
                              product_id TEXT PRIMARY KEY,
                              product_name VARCHAR(50),
                              product_price FLOAT,
                              product_category TEXT,
                              pet_category VARCHAR(50),
                              product_weight FLOAT,
                              product_color VARCHAR(50),
                              product_size VARCHAR(50),
                              product_material VARCHAR(50),
                              product_brand VARCHAR(50),
                              product_description TEXT
);

-- 7. Контакты клиентов
CREATE TABLE customer_contact_info (
                                       customer_id TEXT PRIMARY KEY,
                                       customer_email VARCHAR(50),
                                       customer_country VARCHAR(50),
                                       customer_postal_code VARCHAR(50),
                                       UNIQUE(customer_email)
);

-- 8. Питомцы клиентов
CREATE TABLE customer_pet_info (
                                   customer_id TEXT PRIMARY KEY,
                                   pet_type VARCHAR(50),
                                   pet_name VARCHAR(50),
                                   pet_breed VARCHAR(50)
);

-- 9. Контакты продавцов
CREATE TABLE seller_contact_info (
                                     seller_id TEXT PRIMARY KEY,
                                     seller_email VARCHAR(50),
                                     seller_country VARCHAR(50),
                                     seller_postal_code VARCHAR(50),
                                     UNIQUE(seller_email)
);

-- 10. Информация о магазинах
CREATE TABLE store_info (
                            store_id TEXT PRIMARY KEY,
                            store_state VARCHAR(50),
                            store_country VARCHAR(50),
                            store_phone VARCHAR(50),
                            store_email VARCHAR(50),
                            UNIQUE(store_email)
);

-- 11. Информация о поставщиках
CREATE TABLE supplier_info (
                               supplier_id TEXT PRIMARY KEY,
                               supplier_email VARCHAR(50),
                               supplier_phone VARCHAR(50),
                               supplier_country VARCHAR(50),
                               UNIQUE(supplier_email)
);

-- 12. Статистика товаров
CREATE TABLE product_statistics (
                                    product_id TEXT PRIMARY KEY,
                                    product_rating FLOAT,
                                    product_reviews INT,
                                    product_release_date DATE,
                                    product_expiry_date DATE
);

-- 13. Факты продаж
CREATE TABLE fact_sales (
                            sale_id TEXT PRIMARY KEY,
                            customer_id TEXT NOT NULL,
                            product_id TEXT NOT NULL,
                            seller_id TEXT NOT NULL,
                            store_id TEXT NOT NULL,
                            supplier_id TEXT NOT NULL,
                            sale_date DATE NOT NULL,
                            product_quantity INT NOT NULL,
                            total_amount DECIMAL(10, 2)
);