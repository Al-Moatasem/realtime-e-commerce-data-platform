from datetime import datetime

from core.config import settings
from core.database import get_ch_db_client


class StoreRepository:
    def __init__(self):
        self.client = get_ch_db_client()

    def get_kpis(self, store_id: str, start_time: datetime, end_time: datetime):
        """Fetches KPIs for an exact time window."""
        query = f"""
            SELECT
                sum(total_revenue) AS revenue,
                sum(total_orders) AS orders,
                uniqMerge(unique_visitors_state) AS visitors
            FROM {settings.table_hourly_kpis}
            WHERE store_id = %(store_id)s
              AND ts_hour >= %(start_time)s
              AND ts_hour < %(end_time)s
        """

        parameters = {
            "store_id": store_id,
            "start_time": start_time,
            "end_time": end_time,
        }

        # clickhouse-connect perfectly translates Python datetimes to CH datetimes
        result = self.client.query(query, parameters).first_item

        return {
            "revenue": float(result.get("revenue") or 0.0),
            "orders": int(result.get("orders") or 0),
            "visitors": int(result.get("visitors") or 0),
        }

    def get_traffic_conversion(
        self, store_id: str, start_time: datetime, end_time: datetime, is_today: bool
    ):
        # Group by hour for 'Today', else group by Date
        time_format = "toStartOfHour(ts_hour)" if is_today else "toDate(ts_hour)"

        query = f"""
            SELECT
                toString({time_format}) AS timestamp,
                uniqMerge(unique_visitors_state) AS visits,
                sum(total_orders) AS orders
            FROM {settings.table_hourly_kpis}
            WHERE store_id = %(store_id)s
              AND ts_hour >= %(start_time)s
              AND ts_hour < %(end_time)s
            GROUP BY timestamp
            ORDER BY timestamp ASC
        """
        result = self.client.query(
            query,
            {"store_id": store_id, "start_time": start_time, "end_time": end_time},
        )
        return [
            {
                "timestamp": row["timestamp"],
                "visits": int(row["visits"]),
                "orders": int(row["orders"]),
            }
            for row in result.named_results()
        ]

    def get_top_products(self, store_id: str, start_time: datetime, end_time: datetime):
        query = f"""
            SELECT
                s.product_id AS product_id,
                any(p.product_name) AS product_name,
                sum(s.total_revenue) AS revenue
            FROM {settings.table_daily_sales} AS s
            LEFT JOIN {settings.table_products} AS p ON s.product_id = p.product_id
            WHERE s.store_id = %(store_id)s
              AND s.ts_date >= toDate(%(start_time)s)
              AND s.ts_date <= toDate(%(end_time)s)
            GROUP BY product_id
            ORDER BY revenue DESC
            LIMIT 5
        """
        result = self.client.query(
            query,
            {"store_id": store_id, "start_time": start_time, "end_time": end_time},
        )
        return result.named_results()

    def get_recent_orders(self, store_id: str, limit: int = 5):
        query = f"""
            SELECT
                order_id,
                customer_id,
                total_amount,
                order_status
            FROM {settings.table_orders}
            WHERE store_id = %(store_id)s
            ORDER BY created_at DESC
            LIMIT %(limit)s
        """
        result = self.client.query(query, {"store_id": store_id, "limit": limit})
        return result.named_results()
