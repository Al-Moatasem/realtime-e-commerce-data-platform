{{
    config(
        materialized='materialized_view',
        engine='ReplacingMergeTree(updated_at)',
        order_by=['merchant_id', 'store_id', 'product_id'],
        catchup=False
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source("kafka_pg_app", "raw_kafka_pg_app_products") }}
),

extracted AS (
    SELECT
        JSONExtractString(message, 'payload', 'after','product_id') AS product_id,
        JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
        JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
        JSONExtractString(message, 'payload', 'after','category') AS product_category,
        JSONExtractString(message, 'payload', 'after','sku') AS product_sku,
        JSONExtractString(message, 'payload', 'after','name') AS product_name,
        parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
        parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
    FROM src
)

SELECT * FROM extracted
