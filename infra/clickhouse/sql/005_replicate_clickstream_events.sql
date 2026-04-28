DROP TABLE IF EXISTS dwh.kafka_storefront_clickstream;

CREATE TABLE dwh.kafka_storefront_clickstream
(
    event_name LowCardinality(String),
    session_id String,
    ts DateTime64(3),
    store_id String,
    customer_id String,
    product_id String,
    action LowCardinality(String),
    quantity UInt16,
    unit_price Decimal(10,2),
    cart_item_count UInt16,
    expected_total Decimal(18,2)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (store_id, toDate(ts), event_name, session_id)
;

DROP TABLE IF EXISTS dwh.raw_kafka_storefront_clickstream;

CREATE TABLE dwh.raw_kafka_storefront_clickstream (
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
    kafka_group_name = 'clickhouse_storefront_group',
    kafka_format = 'JSONEachRow'
;

DROP VIEW IF EXISTS dwh.kafka_storefront_clickstream_mv;

CREATE MATERIALIZED VIEW dwh.kafka_storefront_clickstream_mv
TO dwh.kafka_storefront_clickstream
AS
SELECT
    event_name,
    session_id,
    parseDateTime64BestEffort(`timestamp`) AS ts,
    store_id,
    customer_id,
    product_id,
    action,
    quantity,
    unit_price,
    cart_item_count,
    expected_total
FROM dwh.raw_kafka_storefront_clickstream
;
