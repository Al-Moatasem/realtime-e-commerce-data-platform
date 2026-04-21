from core.utils import TimePeriod, get_time_bounds
from domains.store.repository import StoreRepository
from domains.store.schemas import (
    LowStockAlert,
    LowStockResponse,
    RecentOrderRow,
    RecentOrdersResponse,
    StoreKPIMetric,
    StoreKPIsResponse,
    TimeRangeMeta,
    TopProductRow,
    TopProductsResponse,
    TrafficConversionResponse,
)


class StoreService:
    def __init__(self):
        self.repo = StoreRepository()

    def _calculate_growth(self, current: float, previous: float) -> float:
        if previous == 0:
            return 100.0 if current > 0 else 0.0
        return round(((current - previous) / previous) * 100, 2)

    def _calculate_aov(self, revenue: float, orders: int) -> float:
        return round(revenue / orders, 2) if orders > 0 else 0.0

    def get_kpis(self, store_id: str, period: TimePeriod) -> StoreKPIsResponse:

        bounds = get_time_bounds(period)

        current = self.repo.get_kpis(
            store_id, bounds["current_start"], bounds["current_end"]
        )
        previous = self.repo.get_kpis(
            store_id, bounds["prev_start"], bounds["prev_end"]
        )

        current_aov = self._calculate_aov(current["revenue"], current["orders"])
        prev_aov = self._calculate_aov(previous["revenue"], previous["orders"])

        return StoreKPIsResponse(
            time_range=TimeRangeMeta(
                start_date=bounds["current_start"].isoformat(),
                end_date=bounds["current_end"].isoformat(),
            ),
            revenue=StoreKPIMetric(
                value=current["revenue"],
                growth_pct=self._calculate_growth(
                    current["revenue"], previous["revenue"]
                ),
            ),
            orders=StoreKPIMetric(
                value=current["orders"],
                growth_pct=self._calculate_growth(
                    current["orders"], previous["orders"]
                ),
            ),
            visitors=StoreKPIMetric(
                value=current["visitors"],
                growth_pct=self._calculate_growth(
                    current["visitors"], previous["visitors"]
                ),
            ),
            aov=StoreKPIMetric(
                value=current_aov,
                growth_pct=self._calculate_growth(current_aov, prev_aov),
            ),
        )

    def get_traffic_conversion(
        self, store_id: str, period: TimePeriod
    ) -> TrafficConversionResponse:
        bounds = get_time_bounds(period)
        is_today = period == TimePeriod.TODAY

        data = self.repo.get_traffic_conversion(
            store_id, bounds["current_start"], bounds["current_end"], is_today
        )
        return TrafficConversionResponse(data=data)

    def get_top_products(
        self, store_id: str, period: TimePeriod
    ) -> TopProductsResponse:
        bounds = get_time_bounds(period)

        raw_products = self.repo.get_top_products(
            store_id, bounds["current_start"], bounds["current_end"]
        )

        products = [
            TopProductRow(
                product_id=row["product_id"],
                product_name=row["product_name"]
                if row["product_name"]
                else row["product_id"],
                revenue=float(row["revenue"]),
            )
            for row in raw_products
        ]
        return TopProductsResponse(products=products)

    def get_recent_orders(self, store_id: str) -> RecentOrdersResponse:
        # Recent orders: ignore the time filter
        raw_orders = self.repo.get_recent_orders(store_id, limit=5)

        orders = [
            RecentOrderRow(
                order_id=row["order_id"],
                customer_name=row["customer_id"],
                total_amount=float(row["total_amount"]),
                status=row["order_status"],
            )
            for row in raw_orders
        ]
        return RecentOrdersResponse(orders=orders)

    def get_low_stock_alerts(self, store_id: str) -> LowStockResponse:
        # Dummy data for the inventory block
        mock_alerts = [
            LowStockAlert(product_name="DUMMY-NovaSound Earbuds", quantity_left=4),
            LowStockAlert(product_name="DUMMY-Thunder USB-C", quantity_left=12),
            LowStockAlert(product_name="DUMMY-PixelShield Screen", quantity_left=0),
        ]
        return LowStockResponse(alerts=mock_alerts)
