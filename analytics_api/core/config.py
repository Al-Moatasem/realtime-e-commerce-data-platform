from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE_PATH = ROOT_DIR / ".env"


class Settings(BaseSettings):
    debug: bool = False
    app_name: str = "Analytics API"
    environment: str = "development"

    # ClickHouse Credentials
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str
    clickhouse_password: str

    table_hourly_kpis: str = Field(
        "dwh.agg_hourly_merchant_store_kpis",
        alias="CLICKHOUSE_TABLE_HOURLY_KPIS",
    )
    table_daily_sales: str = Field(
        "dwh.agg_daily_product_sales",
        alias="CLICKHOUSE_TABLE_DAILY_SALES",
    )
    table_orders: str = Field(
        "dwh.kafka_pg_app_orders",
        alias="CLICKHOUSE_TABLE_ORDERS",
    )
    table_products: str = Field(
        "dwh.kafka_pg_app_products",
        alias="CLICKHOUSE_TABLE_PRODUCTS",
    )
    table_stores: str = Field(
        "dwh.kafka_pg_app_stores",
        alias="CLICKHOUSE_TABLE_STORES",
    )

    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
