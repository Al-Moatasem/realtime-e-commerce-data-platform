-- Creating the database
DROP DATABASE IF EXISTS dwh_dbt;
CREATE DATABASE IF NOT EXISTS dwh_dbt;


-- Integrating Kafka topics as Tables
-- PG / Stores
DROP TABLE IF EXISTS dwh_dbt.raw_kafka_pg_app_stores;
CREATE TABLE dwh_dbt.raw_kafka_pg_app_stores (
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'pg_src_ecom_db.public.stores',
    kafka_group_name = 'clickhouse_pg_app_stores_dbt_group',
    kafka_format = 'JSONAsString',
    kafka_thread_per_consumer = 0,
    kafka_num_consumers = 1
;

-- PG / Products
DROP TABLE IF EXISTS dwh_dbt.raw_kafka_pg_app_products;
CREATE TABLE dwh_dbt.raw_kafka_pg_app_products (
      message String
)
ENGINE = Kafka
SETTINGS
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'pg_src_ecom_db.public.products',
      kafka_group_name = 'clickhouse_pg_app_products_dbt_group',
      kafka_format = 'JSONAsString',
      kafka_thread_per_consumer = 0,
      kafka_num_consumers = 1
;


-- PG / Orders
DROP TABLE IF EXISTS dwh_dbt.raw_kafka_pg_app_orders;
CREATE TABLE dwh_dbt.raw_kafka_pg_app_orders (
      message String
)
ENGINE = Kafka
SETTINGS
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'pg_src_ecom_db.public.orders',
      kafka_group_name = 'clickhouse_pg_app_orders_dbt_group',
      kafka_format = 'JSONAsString',
      kafka_thread_per_consumer = 0,
      kafka_num_consumers = 1
;

-- PG / Order Lines
DROP TABLE IF EXISTS dwh_dbt.raw_kafka_pg_app_order_lines;
CREATE TABLE dwh_dbt.raw_kafka_pg_app_order_lines (
      message String
)
ENGINE = Kafka
SETTINGS
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'pg_src_ecom_db.public.order_lines',
      kafka_group_name = 'clickhouse_pg_app_order_lines_dbt_group',
      kafka_format = 'JSONAsString',
      kafka_thread_per_consumer = 0,
      kafka_num_consumers = 1
;


-- Storefront / Clickstream
DROP TABLE IF EXISTS dwh_dbt.raw_kafka_storefront_clickstream;
CREATE TABLE dwh_dbt.raw_kafka_storefront_clickstream (
    event_name String,
    session_id String,
    `timestamp` String,
    store_id String,
    customer_id String,
    -- event specific fields
    product_id String,
    action LowCardinality(String),
    quantity UInt16,
    unit_price Decimal,
    cart_item_count UInt16,
    expected_total Decimal
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'storefront.clickstream',
    kafka_group_name = 'clickhouse_storefront_dbt_group',
    kafka_format = 'JSONEachRow'
;
