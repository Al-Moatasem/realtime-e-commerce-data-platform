{{
    config(
        materialized='materialized_view',
        engine='ReplacingMergeTree(updated_at)',
        order_by=['order_id', 'toDate(created_at)', 'order_line_id'],
        catchup=False
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source("kafka_pg_app", "raw_kafka_pg_app_order_lines") }}
),

extracted AS (
    SELECT
        JSONExtractString(message, 'payload', 'after','order_line_id') AS order_line_id,
        JSONExtractString(message, 'payload', 'after','order_id') AS order_id,
        JSONExtractString(message, 'payload', 'after','product_id') AS product_id,
        JSONExtractInt(message, 'payload', 'after','quantity') AS quantity,
        toDecimal64(
            JSONExtractString(message, 'payload', 'after', 'unit_price'), 2
        ) AS unit_price,
        parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
        parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
    FROM src
)

SELECT * FROM extracted
