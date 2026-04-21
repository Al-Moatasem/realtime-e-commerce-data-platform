from pyflink.table import EnvironmentSettings, TableEnvironment


def purchase_analytics_sql_job():

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set(
        "pipeline.name", "SQL Job Event Time - Storefront Click Stream Analytics 01"
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

    table_env.execute_sql(
        """
        CREATE TABLE print_sink (
            event_name STRING,
            session_id STRING,
            event_time STRING,
            store_id STRING,
            customer_id STRING,
            product_id STRING,
            action STRING,
            quantity INT,
            unit_price DOUBLE,
            cart_item_count INT,
            expected_total DOUBLE,
            ts TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )
        """
    )

    table_env.execute_sql(
        """
        INSERT INTO print_sink
        SELECT
            event_name,
            session_id,
            `timestamp` AS event_time,
            store_id,
            customer_id,
            product_id,
            action,
            quantity,
            unit_price,
            cart_item_count,
            expected_total,
            ts
        FROM storefront_clickstream_raw
        """
    ).wait()  # Use .wait() to keep the job running


if __name__ == "__main__":
    purchase_analytics_sql_job()
