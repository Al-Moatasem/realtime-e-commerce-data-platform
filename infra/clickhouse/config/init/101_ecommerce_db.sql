CREATE DATABASE IF NOT EXISTS dwh;
SHOW DATABASES;
USE dwh;

CREATE TABLE IF NOT EXISTS dwh.storefront_events (
    event_name String,
    session_id String,
    ts DateTime64(3),
    store_id String,
    customer_id String,
    product_id String,
    action String,
    quantity INT,
    unit_price DOUBLE,
    cart_item_count INT,
    expected_total DOUBLE
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts) -- monthly partitioning
ORDER BY (store_id, toDate(ts), event_name, ts)
