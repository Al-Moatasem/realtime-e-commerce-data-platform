{{
    config(
        materialized='materialized_view',
        engine='MergeTree',
        partition_by=['toYYYYMM(ts)'],
        order_by=['store_id', 'toDate(ts)', 'event_name', 'session_id'],
        catchup=False
    )
}}
WITH
src AS (
    SELECT *
    FROM {{ source("kafka_clickstream","raw_kafka_storefront_clickstream") }}
),

renamed AS (
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
    FROM src
)

SELECT * FROM renamed
