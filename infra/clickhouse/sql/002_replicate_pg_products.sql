DROP TABLE IF EXISTS dwh.kafka_pg_app_products;

CREATE TABLE dwh.kafka_pg_app_products (
    product_id String,
    merchant_id String,
    store_id String,
    product_sku String,
    product_name String,
    product_category LowCardinality(String),
    unit_price Decimal(10,2),
    created_at DateTime(3),
    updated_at DateTime(3)
    )
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (merchant_id, store_id, product_id)
;

DROP TABLE IF EXISTS dwh.raw_kafka_pg_app_products;

CREATE TABLE dwh.raw_kafka_pg_app_products (
      message String
)
ENGINE = Kafka
SETTINGS
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'pg_src_ecom_db.public.products',
      kafka_group_name = 'clickhouse_pg_app_products_group',
      kafka_format = 'JSONAsString',
      kafka_thread_per_consumer = 0,
      kafka_num_consumers = 1
;

DROP VIEW IF EXISTS dwh.kafka_pg_app_products_mv;

CREATE MATERIALIZED VIEW dwh.kafka_pg_app_products_mv
TO dwh.kafka_pg_app_products
AS
SELECT
    JSONExtractString(message, 'payload', 'after','product_id') AS product_id,
    JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
    JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
    JSONExtractString(message, 'payload', 'after','category') AS product_category,
    JSONExtractString(message, 'payload', 'after','sku') AS product_sku,
    JSONExtractString(message, 'payload', 'after','name') AS product_name,
    parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
    parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at

FROM dwh.raw_kafka_pg_app_products
;
