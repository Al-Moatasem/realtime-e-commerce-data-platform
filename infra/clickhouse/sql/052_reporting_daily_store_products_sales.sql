DROP TABLE IF EXISTS dwh.agg_daily_product_sales;

CREATE TABLE dwh.agg_daily_product_sales
(
    `merchant_id` String,
    `store_id` String,
    `product_id` String,
    `ts_date` Date,
    `total_revenue` SimpleAggregateFunction(sum, Decimal(38,2)),
    `items_sold` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts_date)
ORDER BY (merchant_id, store_id, ts_date, product_id);

DROP VIEW IF EXISTS dwh.agg_daily_product_sales_mv;

CREATE MATERIALIZED VIEW dwh.agg_daily_product_sales_mv
TO dwh.agg_daily_product_sales
AS SELECT
    o.merchant_id AS merchant_id,
    o.store_id AS store_id,
    ol.product_id AS product_id,
    toDate(ol.created_at) AS ts_date,

    sum(toDecimal64(ol.quantity * ol.unit_price, 2)) AS total_revenue,
    sum(ol.quantity) AS items_sold
FROM dwh.kafka_pg_app_order_lines AS ol
LEFT JOIN dwh.kafka_pg_app_orders AS o ON ol.order_id = o.order_id
GROUP BY
    merchant_id,
    store_id,
    product_id,
    ts_date
;

INSERT INTO dwh.agg_daily_product_sales
SELECT
    o.merchant_id AS merchant_id,
    o.store_id AS store_id,
    ol.product_id AS product_id,
    toDate(ol.created_at) AS ts_date,

    sum(toDecimal64(ol.quantity * ol.unit_price, 2)) AS total_revenue,
    sum(ol.quantity) AS items_sold

FROM dwh.kafka_pg_app_order_lines AS ol
LEFT JOIN dwh.kafka_pg_app_orders AS o ON ol.order_id = o.order_id
GROUP BY
    merchant_id,
    store_id,
    product_id,
    ts_date
;
