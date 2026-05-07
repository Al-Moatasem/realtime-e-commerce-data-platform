{{
    config(
        materialized='materialized_view',
        engine='ReplacingMergeTree(updated_at)',
        order_by=['merchant_id', 'store_id'],
        catchup=False
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source("kafka_pg_app", "raw_kafka_pg_app_stores") }}
),

extracted AS (
    SELECT
        JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
        JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
        JSONExtractString(message, 'payload', 'after','region') AS store_region,
        JSONExtractString(message, 'payload', 'after','status') AS store_status,
        JSONExtractString(message, 'payload', 'after','name') AS store_name,
        parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'), 6) AS created_at,
        parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'), 6) AS updated_at
    FROM src
)

SELECT * FROM extracted
