{{
    config(
        materialized='materialized_view',
        engine='ReplacingMergeTree(updated_at)',
        order_by=['merchant_id', 'store_id', 'toDate(created_at)', 'order_id'],
        catchup=False
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source("kafka_pg_app", "raw_kafka_pg_app_orders") }}
),

extracted AS (
    SELECT
        JSONExtractString(message, 'payload', 'after','order_id') AS order_id,
        JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
        JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
        JSONExtractString(message, 'payload', 'after','customer_id') AS customer_id,
        JSONExtractString(message, 'payload', 'after','order_status') AS order_status,

        toDecimal64(
            JSONExtractString(message, 'payload', 'after', 'total_amount'), 2
        ) AS total_amount,

        parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
        parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
    FROM src
)

SELECT * FROM extracted
