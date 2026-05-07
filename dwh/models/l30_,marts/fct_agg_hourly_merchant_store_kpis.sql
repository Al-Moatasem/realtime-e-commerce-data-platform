{{
    config(
        materialized="materialized_view",
        engine="AggregatingMergeTree",
        partition_by=['toYYYYMM(ts_hour)'],
        order_by=['store_id', 'ts_hour']
    )
}}
WITH
src AS (
    SELECT *
    FROM {{ ref("stg_storefront_clickstream") }}
),

aggregated AS (
    SELECT
        CAST(store_id, 'String') AS store_id,
        CAST(toStartOfHour(ts), 'DateTime') AS ts_hour,

        CAST(
            sumIf(expected_total, event_name = 'checkout_started'),
            'SimpleAggregateFunction(sum, Decimal(38,2))'
        ) AS total_revenue,

        CAST(
            countIf(event_name = 'checkout_started'),
            'SimpleAggregateFunction(sum, UInt64)'
        ) AS  total_orders,

        CAST(
            countIf(event_name = 'page_view'),
            'SimpleAggregateFunction(sum, UInt64)'
        ) AS  page_views,

        CAST(
            countIf(event_name = 'cart_action'),
            'SimpleAggregateFunction(sum, UInt64)'
        ) AS  cart_actions,

        CAST(
            uniqIfState(customer_id, event_name = 'checkout_started'),
            'AggregateFunction(uniq, String)'
        ) AS  active_customers_state,

        CAST(
            uniqState(session_id),
            'AggregateFunction(uniq, String)'
        ) AS unique_visitors_state

    FROM src
    GROUP BY
        store_id,
        ts_hour

)

SELECT * FROM aggregated
