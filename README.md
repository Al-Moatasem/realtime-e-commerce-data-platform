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
