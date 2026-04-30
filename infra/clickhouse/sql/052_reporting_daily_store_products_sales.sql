DROP TABLE IF EXISTS dwh.agg_daily_product_sales;

CREATE TABLE dwh.agg_daily_product_sales
(
    `product_id` String,
    `ts_date` Date,
    `total_revenue` SimpleAggregateFunction(sum, Decimal(38,2)),
    `items_sold` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts_date)
ORDER BY (product_id, ts_date);

DROP VIEW IF EXISTS dwh.agg_daily_product_sales_mv;

CREATE MATERIALIZED VIEW dwh.agg_daily_product_sales_mv
TO dwh.agg_daily_product_sales
AS SELECT
    product_id,
    toDate(created_at) AS ts_date,
    sum(toDecimal64(quantity * unit_price, 2)) AS total_revenue,
    sum(quantity) AS items_sold
FROM dwh.kafka_pg_app_order_lines
GROUP BY
    product_id,
    ts_date
;

INSERT INTO dwh.agg_daily_product_sales
SELECT
    product_id,
    toDate(created_at) AS ts_date,
    sum(toDecimal64(quantity * unit_price, 2)) AS total_revenue,
    sum(quantity) AS items_sold
FROM dwh.kafka_pg_app_order_lines
GROUP BY
    product_id,
    ts_date
;
