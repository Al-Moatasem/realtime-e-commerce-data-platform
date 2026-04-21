from domains.merchant.repository import MerchantRepository
from domains.merchant.schemas import (
    FunnelResponse,
    KPIMetric,
    MerchantKPIsResponse,
    RevenueTrendResponse,
    StorePortfolioResponse,
    StorePortfolioRow,
    TimePeriod,
)


class MerchantService:
    def __init__(self):
        self.repo = MerchantRepository()

    def _calculate_growth(self, current: float, previous: float) -> float:
        if previous == 0:
            return 100.0 if current > 0 else 0.0
        return round(((current - previous) / previous) * 100, 2)

    def _calculate_conv_rate(self, orders: int, visits: int) -> float:
        if visits == 0:
            return 0.0
        return round((orders / visits) * 100, 2)

    def get_kpis(
        self,
        merchant_id: str,
        period: TimePeriod,
    ) -> MerchantKPIsResponse:
        # Map period to hours
        hours_map = {
            TimePeriod.TODAY: 24,
            TimePeriod.LAST_7_DAYS: 24 * 7,
            TimePeriod.LAST_30_DAYS: 24 * 30,
        }
        interval = hours_map[period]

        # Fetch data from ClickHouse
        current_data = self.repo.get_kpis_for_period(
            merchant_id, interval_hours=interval, offset_hours=0
        )
        prev_data = self.repo.get_kpis_for_period(
            merchant_id, interval_hours=interval, offset_hours=interval
        )

        # Calculate Conversion Rates
        current_conv_rate = self._calculate_conv_rate(
            current_data["orders"],
            current_data["visits"],
        )
        prev_conv_rate = self._calculate_conv_rate(
            prev_data["orders"],
            prev_data["visits"],
        )

        # Build final response with growth percentages
        return MerchantKPIsResponse(
            total_revenue=KPIMetric(
                value=current_data["revenue"],
                growth_pct=self._calculate_growth(
                    current_data["revenue"],
                    prev_data["revenue"],
                ),
            ),
            total_orders=KPIMetric(
                value=current_data["orders"],
                growth_pct=self._calculate_growth(
                    current_data["orders"],
                    prev_data["orders"],
                ),
            ),
            conversion_rate=KPIMetric(
                value=current_conv_rate,
                growth_pct=self._calculate_growth(current_conv_rate, prev_conv_rate),
            ),
            active_customers=KPIMetric(
                value=current_data["active_customers"],
                growth_pct=self._calculate_growth(
                    current_data["active_customers"],
                    prev_data["active_customers"],
                ),
            ),
        )

    def get_revenue_trend(
        self, merchant_id: str, period: TimePeriod
    ) -> RevenueTrendResponse:
        hours_map = {
            TimePeriod.TODAY: 24,
            TimePeriod.LAST_7_DAYS: 24 * 7,
            TimePeriod.LAST_30_DAYS: 24 * 30,
        }
        trend_data = self.repo.get_revenue_trend(merchant_id, hours_map[period])
        return RevenueTrendResponse(trend=trend_data)

    def get_funnel(self, merchant_id: str, period: TimePeriod) -> FunnelResponse:
        hours_map = {
            TimePeriod.TODAY: 24,
            TimePeriod.LAST_7_DAYS: 24 * 7,
            TimePeriod.LAST_30_DAYS: 24 * 30,
        }
        funnel_data = self.repo.get_funnel(merchant_id, hours_map[period])
        return FunnelResponse(**funnel_data)

    def get_store_portfolio(
        self, merchant_id: str, period: TimePeriod
    ) -> StorePortfolioResponse:
        hours_map = {
            TimePeriod.TODAY: 24,
            TimePeriod.LAST_7_DAYS: 24 * 7,
            TimePeriod.LAST_30_DAYS: 24 * 30,
        }
        interval = hours_map[period]

        # Fetch current and previous period data per store
        current_data = self.repo.get_store_portfolio(
            merchant_id, interval_hours=interval, offset_hours=0
        )
        prev_data = self.repo.get_store_portfolio(
            merchant_id, interval_hours=interval, offset_hours=interval
        )

        portfolio_rows = []
        for store_id, current in current_data.items():
            # Get previous revenue to calculate growth, default to 0 if no prev data
            prev_revenue = float(prev_data.get(store_id, {}).get("revenue") or 0.0)
            current_revenue = float(current.get("revenue") or 0.0)

            growth_pct = self._calculate_growth(current_revenue, prev_revenue)
            conv_rate = self._calculate_conv_rate(
                int(current.get("orders") or 0), int(current.get("visits") or 0)
            )

            portfolio_rows.append(
                StorePortfolioRow(
                    store_id=store_id,
                    store_name=current.get("store_name"),
                    revenue=current_revenue,
                    orders=int(current.get("orders") or 0),
                    conversion_rate=conv_rate,
                    status=current.get("status") or "Active",
                    growth_pct=growth_pct,
                )
            )

        # Sort by highest revenue descending
        portfolio_rows.sort(key=lambda x: x.revenue, reverse=True)
        return StorePortfolioResponse(stores=portfolio_rows)
