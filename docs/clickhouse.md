# ClickHouse

In this project we have different options to how to utilize ClickHouse in our platform
1. Using raw SQL scripts to create tables, materialized views, populate data
2. Integrating dbt framework to the platform


## Using raw SQL Scripts
- When we start the ClickHouse docker service for the first time, a new database named `dwh` has been created.
- In a new terminal, open the ClickHOuse container's terminal then open the ClickHouse client
  ```bash
  docker exec -it clickhouse-server bash

  # the credentials were defined in the docker compose file
  clickhouse client --user admin --password pass_adwin_road
  ```

### Replicating Data from Kafka to ClickHouse
We are going to replicate the Kafka messages into ClickHouse tables (raw zone), create different tables to feed the merchant and store dashboards (reporting zone), create materialized views to populate the tables in reporting zone from the tables in the raw zone

**Replicating the stores Kafka topic**
- First we create a ClickHouse table with the desired table schema
  ```sql
  DROP TABLE IF EXISTS dwh.kafka_pg_app_stores;
  CREATE TABLE dwh.kafka_pg_app_stores (
      store_id String,
      merchant_id String,
      store_region LowCardinality(String),
      store_status LowCardinality(String),
      store_name String,
      created_at DateTime(3),
      updated_at DateTime(3)
      )
  ENGINE = ReplacingMergeTree(updated_at)
  ORDER BY (merchant_id, store_id)
  ;
  ```
- Establish a connection between Kafka and ClickHouse, that table doesn't store data
  ```sql
  DROP TABLE IF EXISTS dwh.raw_kafka_pg_app_stores;
  CREATE TABLE dwh.raw_kafka_pg_app_stores (
       message String
  )
  ENGINE = Kafka
  SETTINGS
       kafka_broker_list = 'kafka:29092',
       kafka_topic_list = 'pg_src_ecom_db.public.stores',
       kafka_group_name = 'clickhouse_pg_app_stores_group',
       kafka_format = 'JSONAsString',
       kafka_thread_per_consumer = 0,
       kafka_num_consumers = 1
  ;
  ```
- We can list the created tables
  ```sql
  USE dwh;
  SHOW tables;
  ```
- A materialized view that will pull data from the raw table `dwh.raw_kafka_pg_app_stores` (raw table consume from Kafka topic) and insert it to the reporting table `dwh.kafka_pg_app_stores`. The View will parse the Kafka message (json format, and extract the attributes/keys we are interested in)
  ```sql
  DROP VIEW IF EXISTS dwh.kafka_pg_app_stores_mv;
  CREATE MATERIALIZED VIEW dwh.kafka_pg_app_stores_mv
  TO dwh.kafka_pg_app_stores
  AS
  SELECT
      JSONExtractString(message, 'payload', 'after','store_id') AS store_id,
      JSONExtractString(message, 'payload', 'after','merchant_id') AS merchant_id,
      JSONExtractString(message, 'payload', 'after','region') AS store_region,
      JSONExtractString(message, 'payload', 'after','status') AS store_status,
      JSONExtractString(message, 'payload', 'after','name') AS store_name,
      parseDateTime64BestEffort(JSONExtractString(message,'payload','after', 'created_at'),6) AS created_at,
      parseDateTime64BestEffort(JSONExtractString(message, 'payload','after', 'updated_at'),6) AS updated_at
  FROM dwh.raw_kafka_pg_app_stores
  ;
  ```
- Check the data in the table (we may need to wait a bit till data got replicated)
  ```sql
  SELECT COUNT(*)  FROM dwh.kafka_pg_app_stores;
  ```

---

We will use the same approach for the remaining Kafka topics, but instead of copying and pasting queries into the ClickHouse CLI, we will execute them from `.sql` files. All queries are saved under `./infra/clickhouse/sql`, and that directory is mounted inside the container at `/opt/ch/sql`


- Exit from the ClickHouse client terminal by typing `exit` and press enter
- Execute these commands (in the clickhouse-server container)
  ```bash
  clickhouse-client --multiquery < /opt/ch/sql/002_replicate_pg_products.sql
  clickhouse-client --multiquery < /opt/ch/sql/003_replicate_pg_orders.sql
  clickhouse-client --multiquery < /opt/ch/sql/004_replicate_pg_order_lines.sql
  clickhouse-client --multiquery < /opt/ch/sql/005_replicate_clickstream_events.sql
  ```

- We can check the created tables, and the count of records replicated, first open the ClickHouse client terminal, then run the queries
  ```bash
  clickhouse-client
  ```
  ```sql
  USE dwh;
  SHOW tables;
  SELECT COUNT(*) FROM dwh.kafka_pg_app_products;
  SELECT COUNT(*) FROM dwh.kafka_pg_app_orders
  SELECT COUNT(*) FROM dwh.kafka_pg_app_order_lines
  SELECT COUNT(*) FROM dwh.kafka_storefront_clickstream;
  ```

### Creating Tables for Reporting Layer

#### Store Hourly KPIs
- The AggregatingMergeTree definition
  ```sql
  DROP TABLE IF EXISTS dwh.agg_hourly_merchant_store_kpis;
  CREATE TABLE dwh.agg_hourly_merchant_store_kpis
  (
      `store_id` String,
      `ts_hour` DateTime,

      `total_revenue` SimpleAggregateFunction(sum, Decimal(38,2)),
      `total_orders` SimpleAggregateFunction(sum, UInt64),
      `page_views` SimpleAggregateFunction(sum, UInt64),
      `cart_actions` SimpleAggregateFunction(sum, UInt64),

      `active_customers_state` AggregateFunction(uniq, String),
      `unique_visitors_state` AggregateFunction(uniq, String)
  )
  ENGINE = AggregatingMergeTree()
  PARTITION BY toYYYYMM(ts_hour)
  ORDER BY (store_id, ts_hour);
  ```
- The materialized view to populate the table (works with newly inserted data)
  ```sql
  DROP VIEW IF EXISTS dwh.agg_hourly_merchant_store_kpis_mv;
  CREATE MATERIALIZED VIEW dwh.agg_hourly_merchant_store_kpis_mv
  TO dwh.agg_hourly_merchant_store_kpis
  AS SELECT
      store_id,
      toStartOfHour(ts) AS ts_hour,

      /*sum(if(event_name = 'checkout_started', expected_total, 0)) AS total_revenue,*/
      sumIf(expected_total, event_name = 'checkout_started') AS total_revenue,

      /* count(if(event_name = 'checkout_started', 1, NULL)) AS total_orders,*/
      countIf(event_name = 'checkout_started') AS total_orders,

      countIf(event_name = 'page_view') AS page_views,
      countIf(event_name = 'cart_action') AS cart_actions,

      uniqIfState(customer_id, event_name = 'checkout_started') AS active_customers_state,
      uniqState(session_id) AS unique_visitors_state

  FROM dwh.kafka_storefront_clickstream
  GROUP BY
      store_id,
      ts_hour;
  ```

- Populating the table with current data (This approach works in our case, as we can stop data generation, in a production environment, we need to apply a suitable backfilling approach)
  ```sql
  INSERT INTO dwh.agg_hourly_merchant_store_kpis
  SELECT
      store_id,
      toStartOfHour(ts) AS ts_hour,

      sumIf(expected_total, event_name = 'checkout_started') AS total_revenue,
      countIf(event_name = 'checkout_started') AS total_orders,
      countIf(event_name = 'page_view') AS page_views,
      countIf(event_name = 'cart_action') AS cart_actions,

      uniqIfState(customer_id, event_name = 'checkout_started') AS active_customers_state,
      uniqState(session_id) AS unique_visitors_state

  FROM dwh.kafka_storefront_clickstream
  GROUP BY
      store_id,
      ts_hour;
  ```
#### Store / Product Daily Sales
```sql
DROP TABLE IF EXISTS dwh.agg_daily_product_sales;
CREATE TABLE dwh.agg_daily_product_sales
(
    `product_id` String,
    `ts_date` Date,
    `total_revenue` SimpleAggregateFunction(sum, Decimal(38,2)),
    `items_sold` SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(ts_date)
ORDER BY (product_id, ts_date);
```
```sql
DROP VIEW IF EXISTS dwh.agg_daily_product_sales_mv;
CREATE MATERIALIZED VIEW dwh.agg_daily_product_sales_mv
TO dwh.agg_daily_product_sales
AS SELECT
    product_id,
    toDate(created_at) AS ts_date,
    sum(toDecimal64(quantity * unit_price, 2)) AS total_revenue,
    sum(quantity) AS items_sold
FROM dwh.kafka_pg_app_order_lines
GROUP BY
    product_id,
    ts_date
;
```
populating the daily agg table
```sql
INSERT INTO dwh.agg_daily_product_sales
SELECT
    product_id,
    toDate(created_at) AS ts_date,
    sum(toDecimal64(quantity * unit_price, 2)) AS total_revenue,
    sum(quantity) AS items_sold
FROM dwh.kafka_pg_app_order_lines
GROUP BY
    product_id,
    ts_date;
```
