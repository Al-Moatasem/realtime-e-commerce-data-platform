from core.config import settings
from core.database import get_ch_db_client


class MerchantRepository:
    def __init__(self):
        self.client = get_ch_db_client()

    def get_kpis_for_period(
        self, merchant_id: str, interval_hours: int, offset_hours: int = 0
    ):
        """
        Fetches KPIs for a specific time window.
        interval_hours: How far back to look from the offset.
        offset_hours: How far back to shift the end date (used for previous period).
        """
        query = f"""
            SELECT
                sum(total_revenue) AS revenue,
                sum(total_orders) AS orders,
                sum(page_views) AS visits,
                uniqMerge(active_customers_state) AS active_customers
            FROM {settings.table_hourly_kpis}
            WHERE merchant_id = %(merchant_id)s
              AND ts_hour >= now() - INTERVAL %(total_lookback)s HOUR
              AND ts_hour < now() - INTERVAL %(offset_hours)s HOUR
        """

        parameters = {
            "merchant_id": merchant_id,
            "total_lookback": interval_hours + offset_hours,
            "offset_hours": offset_hours,
        }

        # clickhouse_connect returns a list of dictionaries if we ask it to
        result = self.client.query(query, parameters).first_item

        # Handle nulls if no data exists for the period
        return {
            "revenue": float(result.get("revenue") or 0.0),
            "orders": int(result.get("orders") or 0),
            "visits": int(result.get("visits") or 0),
            "active_customers": int(result.get("active_customers") or 0),
        }

    def get_revenue_trend(self, merchant_id: str, interval_hours: int):
        """Fetches time-series data for the line chart."""
        # Group by hour if viewing "Today", else group by Date
        time_format = (
            "toStartOfHour(ts_hour)" if interval_hours <= 24 else "toDate(ts_hour)"
        )

        query = f"""
            SELECT
                toString({time_format}) AS timestamp,
                sum(total_revenue) AS revenue
            FROM {settings.table_hourly_kpis}
            WHERE merchant_id = %(merchant_id)s
              AND ts_hour >= now() - INTERVAL %(interval_hours)s HOUR
            GROUP BY timestamp
            ORDER BY timestamp ASC
        """
        result = self.client.query(
            query, {"merchant_id": merchant_id, "interval_hours": interval_hours}
        )
        return [
            {"timestamp": row["timestamp"], "revenue": float(row["revenue"])}
            for row in result.named_results()
        ]

    def get_funnel(self, merchant_id: str, interval_hours: int):
        """Fetches aggregates for the Funnel."""
        query = f"""
            SELECT
                sum(page_views) AS visits,
                sum(cart_actions) AS add_to_cart,
                sum(total_orders) AS checkout
            FROM {settings.table_hourly_kpis}
            WHERE merchant_id = %(merchant_id)s
              AND ts_hour >= now() - INTERVAL %(interval_hours)s HOUR
        """
        result = self.client.query(
            query, {"merchant_id": merchant_id, "interval_hours": interval_hours}
        ).first_item
        return {
            "visits": int(result.get("visits") or 0),
            "add_to_cart": int(result.get("add_to_cart") or 0),
            "checkout": int(result.get("checkout") or 0),
        }

    def get_store_portfolio(
        self, merchant_id: str, interval_hours: int, offset_hours: int = 0
    ):
        """Fetches KPIs grouped by individual stores, joining with the stores table for status."""
        query = f"""
            SELECT
                k.store_id AS store_id,
                any(s.store_name) AS store_name,
                any(s.store_status) AS status,
                sum(k.total_revenue) AS revenue,
                sum(k.total_orders) AS orders,
                sum(k.page_views) AS visits
            FROM {settings.table_hourly_kpis} AS k
            LEFT JOIN {settings.table_stores} AS s ON k.store_id = s.store_id
            WHERE k.merchant_id = %(merchant_id)s
              AND k.ts_hour >= now() - INTERVAL %(total_lookback)s HOUR
              AND k.ts_hour < now() - INTERVAL %(offset_hours)s HOUR
            GROUP BY store_id
        """
        params = {
            "merchant_id": merchant_id,
            "total_lookback": interval_hours + offset_hours,
            "offset_hours": offset_hours,
        }
        result = self.client.query(query, params)
        return {row["store_id"]: row for row in result.named_results()}
