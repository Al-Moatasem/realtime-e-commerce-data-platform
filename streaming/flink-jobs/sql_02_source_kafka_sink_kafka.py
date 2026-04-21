from pyflink.table import EnvironmentSettings, TableEnvironment


def purchase_analytics_sql_job():

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set(
        "pipeline.name", "SQL Job Event Time - Storefront Click Stream Analytics 02"
    )

    table_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS storefront_clickstream_raw  (
            event_name STRING,
            session_id STRING,
            `timestamp` STRING,
            store_id STRING,
            customer_id STRING,
            `ts` AS TO_TIMESTAMP(
                REPLACE(`timestamp`, 'Z', ''),
                'yyyy-MM-dd''T''HH:mm:ss.SSSSSS'
            ),
            -- Define the watermark strategy.
            WATERMARK FOR `ts` AS `ts` - INTERVAL '5' SECOND,

            -- event specific fields
            product_id STRING,
            action STRING,
            quantity INT,
            unit_price DOUBLE,
            cart_item_count INT,
            expected_total DOUBLE
        ) WITH (
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'connector' = 'kafka',
            'topic' = 'storefront.clickstream',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'storefront-clickstream-raw-sql-group',
            'scan.startup.mode' = 'latest-offset'
        )
        """
    )

    # The Sink Table
    table_env.execute_sql(
        """
        CREATE TABLE IF NOT EXISTS store_analytics (
            store_id STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            page_views_count BIGINT,
            customers_count BIGINT
        ) WITH (
            'format' = 'json',
            'key.format' = 'raw',
            'key.fields' = 'store_id',
            'connector' = 'kafka',
            'topic' = 'store_analytics_stream',
            'properties.bootstrap.servers' = 'kafka:29092'
        )
        """
    )

    table_env.execute_sql(
        """
        INSERT INTO store_analytics
        SELECT
            store_id,
            TUMBLE_START(ts, INTERVAL '1' MINUTE) as window_start,
            TUMBLE_END(ts, INTERVAL '1' MINUTE) as window_end,
            COUNT(*) as page_views_count,
            COUNT(DISTINCT customer_id) as customers_count
        FROM storefront_clickstream_raw
        GROUP BY
            store_id,
            TUMBLE(ts, INTERVAL '1' MINUTE)
    """
    ).wait()  # Use .wait() to keep the job running


if __name__ == "__main__":
    purchase_analytics_sql_job()
