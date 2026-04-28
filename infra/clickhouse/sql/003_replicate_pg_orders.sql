DROP TABLE IF EXISTS dwh.kafka_pg_app_orders;

CREATE TABLE dwh.kafka_pg_app_orders
(
    order_id String,
    merchant_id String,
    store_id String,
    customer_id String,
    order_status String,
    total_amount Decimal(10,2),
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (merchant_id, store_id, toDate(created_at), order_id)
;

DROP TABLE IF EXISTS dwh.raw_kafka_pg_app_orders;

CREATE TABLE dwh.raw_kafka_pg_app_orders (
      message String
)
ENGINE = Kafka
SETTINGS
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'pg_src_ecom_db.public.orders',
      kafka_group_name = 'clickhouse_pg_app_orders_group',
      kafka_format = 'JSONAsString',
      kafka_thread_per_consumer = 0,
      kafka_num_consumers = 1
;

DROP VIEW IF EXISTS dwh.kafka_pg_app_orders_mv;

CREATE MATERIALIZED VIEW dwh.kafka_pg_app_orders_mv
TO dwh.kafka_pg_app_orders
AS
SELECT
    JSONExtractString(message, 'payload', 'after','order_id') AS order_id,
    JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
    JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
    JSONExtractString(message, 'payload', 'after','customer_id') AS customer_id,
    JSONExtractString(message, 'payload', 'after','order_status') AS order_status,

    -- Avoid using JSONExtractFloat as it imprecise
    toDecimal64(
      JSONExtractString(message, 'payload', 'after', 'total_amount'), 2
    ) AS total_amount,


    parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
    parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
FROM dwh.raw_kafka_pg_app_orders
;
