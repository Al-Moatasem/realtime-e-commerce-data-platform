from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT_DIR = Path(__file__).resolve().parent.parent.parent
ENV_FILE_PATH = ROOT_DIR / ".env"


class Settings(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "ecommerce_db"

    KAFKA_BOOTSTRAP_SERVERS: str = Field(
        default="localhost:9092",
        alias="KAFKA_BOOTSTRAP_SERVERS",
    )
    DEFAULT_SIMULATION_INTERVAL_SECONDS: int = 5
    DEBUG: bool = True
    APP_NAME: str = "E-Commerce Stateful Data Generator"

    # Use the absolute path detected above
    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def DATABASE_URL(self):
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    # Business logic constants
    PRODUCT_CATEGORIES: list[str] = [
        "groceries_essential",
        "fashion_apparel",
        "electronics_tech",
        "health_beauty",
        "home_furniture",
        "pet_supplies",
        "luxury_jewelry",
        "books_media",
        "sports_outdoors",
        "automotive_parts",
    ]


settings = Settings()
