DROP TABLE IF EXISTS dwh.kafka_pg_app_order_lines;

CREATE TABLE dwh.kafka_pg_app_order_lines
(
    order_line_id String,
    order_id String,
    product_id String,
    quantity UInt32,
    unit_price Decimal(10,2),
    created_at DateTime64(3),
    updated_at DateTime64(3)
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(created_at)
ORDER BY (order_id, toDate(created_at), order_line_id)
;

DROP TABLE IF EXISTS dwh.raw_kafka_pg_app_order_lines;

CREATE TABLE dwh.raw_kafka_pg_app_order_lines (
      message String
)
ENGINE = Kafka
SETTINGS
      kafka_broker_list = 'kafka:29092',
      kafka_topic_list = 'pg_src_ecom_db.public.order_lines',
      kafka_group_name = 'clickhouse_pg_app_order_lines_group',
      kafka_format = 'JSONAsString',
      kafka_thread_per_consumer = 0,
      kafka_num_consumers = 1
;

DROP VIEW IF EXISTS dwh.kafka_pg_app_order_lines_mv;

CREATE MATERIALIZED VIEW dwh.kafka_pg_app_order_lines_mv
TO dwh.kafka_pg_app_order_lines
AS
SELECT
    JSONExtractString(message, 'payload', 'after','order_line_id') AS order_line_id,
    JSONExtractString(message, 'payload', 'after','order_id') AS order_id,
    JSONExtractString(message, 'payload', 'after','product_id') AS product_id,
    JSONExtractInt(message, 'payload', 'after','quantity') AS quantity,
    toDecimal64(
      JSONExtractString(message, 'payload', 'after', 'unit_price'), 2
    ) AS unit_price,
    parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
    parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
FROM dwh.raw_kafka_pg_app_order_lines
;
