from pyflink.table import EnvironmentSettings, TableEnvironment


def storefront_events_clickhouse_sql_job():

    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(env_settings)
    table_env.get_config().set(
        "pipeline.name", "SQL Job Event Time - Storefront Click Stream 03 ClickHouse"
    )
    jars = [
        "file:///opt/flink/usrlib/flink-connector-jdbc-core-4.0.0.jar",
        "file:///opt/flink/usrlib/flink-connector-jdbc-mysql-4.0.0.jar",
        "file:///opt/flink/usrlib/flink-json-2.0.0.jar",
        "file:///opt/flink/usrlib/flink-sql-connector-kafka-4.0.0-2.0.jar",
        "file:///opt/flink/usrlib/mysql-connector-j-8.0.33.jar",
    ]
    table_env.get_config().set("pipeline.jars", ";".join(jars))

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
    connector_url = (
        "jdbc:mysql://clickhouse-server:9004/dwh?allowPublicKeyRetrieval=true"
    )

    table_env.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS storefront_events (
            event_name STRING,
            session_id STRING,
            store_id STRING,
            customer_id STRING,
            `ts` TIMESTAMP(3),
            product_id STRING,
            action STRING,
            quantity INT,
            unit_price DOUBLE,
            cart_item_count INT,
            expected_total DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{connector_url}',
            'table-name' = 'storefront_events',
            'driver' = 'com.mysql.cj.jdbc.Driver',
            'username' = 'admin',
            'password' = 'pass_adwin_road',
            'sink.buffer-flush.max-rows' = '50000',
            'sink.buffer-flush.interval' = '5s'
        )
        """
    )

    table_env.execute_sql(
        """
        INSERT INTO storefront_events
        SELECT
            event_name,
            session_id,
            store_id,
            customer_id,
            ts,
            product_id,
            action,
            quantity,
            unit_price,
            cart_item_count,
            expected_total
        FROM storefront_clickstream_raw
    """
    ).wait()  # Use .wait() to keep the job running


if __name__ == "__main__":
    storefront_events_clickhouse_sql_job()
