DROP TABLE IF EXISTS dwh.kafka_pg_app_stores;

CREATE TABLE dwh.kafka_pg_app_stores (
    store_id String,
    merchant_id String,
    store_region LowCardinality(String),
    store_status LowCardinality(String),
    store_name String,
    created_at DateTime(3),
    updated_at DateTime(3)
    )
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (merchant_id, store_id)
;

DROP TABLE IF EXISTS dwh.raw_kafka_pg_app_stores;

CREATE TABLE dwh.raw_kafka_pg_app_stores (
    message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'pg_src_ecom_db.public.stores',
    kafka_group_name = 'clickhouse_pg_app_stores_group',
    kafka_format = 'JSONAsString',
    kafka_thread_per_consumer = 0,
    kafka_num_consumers = 1
;

DROP VIEW IF EXISTS dwh.kafka_pg_app_stores_mv;

CREATE MATERIALIZED VIEW dwh.kafka_pg_app_stores_mv
TO dwh.kafka_pg_app_stores
AS
SELECT
    JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
    JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
    JSONExtractString(message, 'payload', 'after','region') AS store_region,
    JSONExtractString(message, 'payload', 'after','status') AS store_status,
    JSONExtractString(message, 'payload', 'after','name') AS store_name,
    parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
    parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
FROM dwh.raw_kafka_pg_app_stores
;
