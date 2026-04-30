DROP TABLE IF EXISTS dwh.agg_hourly_merchant_store_kpis;

CREATE TABLE dwh.agg_hourly_merchant_store_kpis
(
    `store_id` String,
    `ts_hour` DateTime,

    `total_revenue` SimpleAggregateFunction(sum, Decimal(38,2)),
    `total_orders` SimpleAggregateFunction(sum, UInt64),
    `page_views` SimpleAggregateFunction(sum, UInt64),
    `cart_actions` SimpleAggregateFunction(sum, UInt64),

    `active_customers_state` AggregateFunction(uniq, String),
    `unique_visitors_state` AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts_hour)
ORDER BY (store_id, ts_hour);

DROP VIEW IF EXISTS dwh.agg_hourly_merchant_store_kpis_mv;
CREATE MATERIALIZED VIEW dwh.agg_hourly_merchant_store_kpis_mv
TO dwh.agg_hourly_merchant_store_kpis
AS SELECT
    store_id,
    toStartOfHour(ts) AS ts_hour,

    /*sum(if(event_name = 'checkout_started', expected_total, 0)) AS total_revenue,*/
    sumIf(expected_total, event_name = 'checkout_started') AS total_revenue,

    /* count(if(event_name = 'checkout_started', 1, NULL)) AS total_orders,*/
    countIf(event_name = 'checkout_started') AS total_orders,

    countIf(event_name = 'page_view') AS page_views,
    countIf(event_name = 'cart_action') AS cart_actions,

    uniqIfState(customer_id, event_name = 'checkout_started') AS active_customers_state,
    uniqState(session_id) AS unique_visitors_state

FROM dwh.kafka_storefront_clickstream
GROUP BY
    store_id,
    ts_hour
;


INSERT INTO dwh.agg_hourly_merchant_store_kpis
SELECT
    store_id,
    toStartOfHour(ts) AS ts_hour,

    sumIf(expected_total, event_name = 'checkout_started') AS total_revenue,
    countIf(event_name = 'checkout_started') AS total_orders,
    countIf(event_name = 'page_view') AS page_views,
    countIf(event_name = 'cart_action') AS cart_actions,

    uniqIfState(customer_id, event_name = 'checkout_started') AS active_customers_state,
    uniqState(session_id) AS unique_visitors_state

FROM dwh.kafka_storefront_clickstream
GROUP BY
    store_id,
    ts_hour
;
