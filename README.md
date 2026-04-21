# Real-Time E-Commerce Data Platform

## About This Project
This is an **experimental sandbox project**. I built it to learn and practice modern data engineering tools. The goal is to mimic the real-time data architecture of high-growth e-commerce platforms.

It captures database changes (CDC) and user clickstream events, processes them in real-time, and stores them in a fast analytical database.

## Architecture & Tech Stack
- **Transactional Database:** PostgreSQL
- **Change Data Capture (CDC):** Debezium
- **Message Broker:** Apache Kafka
- **Stream Processing:** Apache Flink
- **OLAP Data Warehouse:** ClickHouse


## How to Use?

- Clone the project
  ```bash
  git clone https://github.com/Al-Moatasem/realtime-e-commerce-data-platform.git
  ```
- Copy the `.env.example` to `.env` and update its values
- Start the docker services
  ```bash
  # Run necessary docker services (excluding flink)
  docker compose -f infra/docker_compose/docker_compose.yaml up db kafka kafka-ui connect clickhouse -d

  # Or, Run all services
  docker compose -f infra/docker_compose/docker_compose.yaml up -d
  ```
  - The docker file include two services for Apache Flink, Flink is not used heavily on this project at this point (we only tested the connection from Flink to Kafka and ClickHouse)
  - The `db` service will
    - Create a postgres database named `ecommerce_db`
    - Create the application tables (merchants, stores, products, ...). (check: `infra\postgres\init\01_application_tables.sql`)
    - Create `debezium_user` user for the CDC tasks (check: `infra\postgres\init\02_debezium_user.sql`).
  - The `clickhouse` service will
    - Create a database named `dwh`, with one or more tables (check: `infra\clickhouse\config\init`)


### Web Applications - Data Faker
We have two separate FastAPI applications:
1. generating random clickstream events and push them into Kafka, and insert necessary records into a Postgres database
2. Consuming data from the ClickHouse database and feed the Merchant and Store dashboards.

---

- Navigate to the `data_faker` directory and create the Python virtual environment
  ```bash
  cd data_faker
  uv sync
  .venv/scripts/activate # Linux: source .venv/bin/activate
  ```
- Start the data faker application
  ```bash
  uv run uvicorn main:app --port 8000 --reload
  ```
- Open the data faker web application `http://localhost:8000`
  - The application depends on the Kafka service, if the Kafka Docker service was not yet started, the UI will display a warning, wait till the Kafka Service is up and running, then restart the data faker application (open `data_faker/main.py` and press **ctrl+s** to trigger the restart)

  ![alt text](docs/assets/data_faker_app_ui.png)

- We have different option to generate data
  - Use the play button for **MERCHANT TRAFFIC & SIMULATION** card to generate a random merchant with one/multiple stores, products, customers, orders on the Postgres database, and publish click events to Kafka, it will prompt a window with number of days for the historical data (default is 65 days)
    - We can trigger multiple data generation actions in the same time
  - Generate a continuous stream of events
    1. Seed the database with merchants, stores, products, customers through the **BULK DATA INITIALIZATION** play button
    2. Generate clickstream events through the **SESSION SIMULATOR** play button.
    3. Start a process that update the order status on the Postgres database through the **ORDER FULFILLMENT** play button.

    > We can run both session simulator and order fulfillment in the same time

### Kafka Connect / Debezium
- While replicating Postgres data into Kafka through Debezium, the default configurations will result in receiving only the `after` state, if we need to receive both the `before` and `after` states of the records, we need to set the replica set to true per table
  - Open the postgres container terminal and access the `psql`
    ```bash
    docker exec -it db bash
    psql -U app_admin -d ecommerce_db -w

    ```
  - Generate the SQL statements for available tables
    ```sql
    SELECT 'ALTER TABLE ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || ' REPLICA IDENTITY FULL;'
    FROM pg_tables
    WHERE TRUE
        AND schemaname IN ('public')
        AND tablename NOT IN ('alembic_version')
    ;
    ```
  - Execute the result queries (the below queries)
    ```sql
    ALTER TABLE public.merchants REPLICA IDENTITY FULL;
    ALTER TABLE public.customers REPLICA IDENTITY FULL;
    ALTER TABLE public.orders REPLICA IDENTITY FULL;
    ALTER TABLE public.stores REPLICA IDENTITY FULL;
    ALTER TABLE public.subscriptions REPLICA IDENTITY FULL;
    ALTER TABLE public.order_lines REPLICA IDENTITY FULL;
    ALTER TABLE public.products REPLICA IDENTITY FULL;
    ```
- create the cdc connector that replicate data from Postgres db into Kafka
  ```bash
  curl -i -X PUT "http://localhost:8083/connectors/jdbc-pg-source-ecom-db/config" \
      -H "Accept:application/json" -H "Content-Type:application/json" \
      -d @streaming/connect/01_jdbc_source_postgres_ecommerce.json
  ```
  The expected response should be like this
  ```json
  {"name":"jdbc-pg-source-ecom-db","config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector",..."name":"jdbc-pg-source-ecom-db"},"tasks":[],"type":"source"}
  ```
- Check the status of the connector and its task
  ```bash
  curl -s "http://localhost:8083/connectors/jdbc-pg-source-ecom-db/status"
  ```
  expected response (we should expect `state = RUNNING` for both the connector and the task)
  ```json
  {
    "name":"jdbc-pg-source-ecom-db",
    "connector":{ "state":"RUNNING","worker_id":"172.18.0.6:8083","version":"3.5.0.Final"},
    "tasks":[{"id":0,"state":"RUNNING","worker_id":"172.18.0.6:8083","version":"3.5.0.Final"}],
    "type":"source"
  }
  ```
- We can check the created topics and replicated records through [kafka-ui](http://localhost:8080/ui/clusters/local-kafka/all-topics) (`http://localhost:8080/ui/clusters/local-kafka/all-topics`)

  ![alt text](docs/assets/kafka-ui-topics-list-01.png)
