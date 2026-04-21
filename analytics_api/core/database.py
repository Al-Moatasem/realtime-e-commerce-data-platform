import clickhouse_connect
from core.config import settings


def get_ch_db_client():
    """
    Creates a fresh ClickHouse client instance per request.
    clickhouse-connect manages an HTTP connection pool under the hood,
    so this is lightweight, fast, and thread-safe for concurrent API calls.
    """
    return clickhouse_connect.get_client(
        host=settings.clickhouse_host,
        port=settings.clickhouse_port,
        username=settings.clickhouse_user,
        password=settings.clickhouse_password,
    )
