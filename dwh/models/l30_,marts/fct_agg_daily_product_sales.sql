{{
    config(
        materialized="materialized_view",
        engine="AggregatingMergeTree",
        partition_by=['toYYYYMM(ts_date)'],
        order_by=['product_id', 'ts_date']
    )
}}
WITH
src AS (
    SELECT *
    FROM {{ ref("stg_pg_app_order_lines") }}
),

aggregated AS (
    SELECT
        CAST(product_id, 'String') AS product_id,
        CAST(toDate(created_at), 'DateTime') AS ts_date,

        CAST(
            sum(toDecimal64(quantity * unit_price, 2)),
            'SimpleAggregateFunction(sum, Decimal(38,2))'
        ) AS total_revenue,

        CAST(
            sum(quantity),
            'SimpleAggregateFunction(sum, UInt64)'
        ) AS  items_sold

    FROM src
    GROUP BY
        product_id,
        ts_date

)

SELECT * FROM aggregated
