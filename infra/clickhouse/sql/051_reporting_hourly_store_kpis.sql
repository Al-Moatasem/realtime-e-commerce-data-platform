DROP TABLE IF EXISTS dwh.agg_hourly_merchant_store_kpis;

CREATE TABLE dwh.agg_hourly_merchant_store_kpis
(
    `merchant_id` String,
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
ORDER BY (merchant_id, store_id, ts_hour);

DROP VIEW IF EXISTS dwh.agg_hourly_merchant_store_kpis_mv;
CREATE MATERIALIZED VIEW dwh.agg_hourly_merchant_store_kpis_mv
TO dwh.agg_hourly_merchant_store_kpis
AS SELECT
    s.merchant_id AS merchant_id,
    e.store_id AS store_id,
    toStartOfHour(e.ts) AS ts_hour,

    /*sum(if(e.event_name = 'checkout_started', e.expected_total, 0)) AS total_revenue,*/
    sumIf(e.expected_total, e.event_name = 'checkout_started') AS total_revenue,

    /* count(if(e.event_name = 'checkout_started', 1, NULL)) AS total_orders,*/
    countIf(e.event_name = 'checkout_started') AS total_orders,

    countIf(e.event_name = 'page_view') AS page_views,
    countIf(e.event_name = 'cart_action') AS cart_actions,

    uniqIfState(e.customer_id, e.event_name = 'checkout_started') AS active_customers_state,
    uniqState(e.session_id) AS unique_visitors_state

FROM dwh.kafka_storefront_clickstream AS e
LEFT JOIN dwh.kafka_pg_app_stores AS s ON e.store_id = s.store_id
GROUP BY
    merchant_id,
    store_id,
    ts_hour
;


INSERT INTO dwh.agg_hourly_merchant_store_kpis
SELECT
    s.merchant_id AS merchant_id,
    e.store_id AS store_id,
    toStartOfHour(e.ts) AS ts_hour,

    sumIf(e.expected_total, e.event_name = 'checkout_started') AS total_revenue,
    countIf(e.event_name = 'checkout_started') AS total_orders,
    countIf(e.event_name = 'page_view') AS page_views,
    countIf(e.event_name = 'cart_action') AS cart_actions,

    uniqIfState(e.customer_id, e.event_name = 'checkout_started') AS active_customers_state,
    uniqState(e.session_id) AS unique_visitors_state

FROM dwh.kafka_storefront_clickstream AS e
LEFT JOIN dwh.kafka_pg_app_stores AS s ON e.store_id = s.store_id
GROUP BY
    merchant_id,
    store_id,
    ts_hour
;
