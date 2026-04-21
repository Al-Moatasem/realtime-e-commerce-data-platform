from enum import Enum
from typing import List

from pydantic import BaseModel


class TimePeriod(str, Enum):
    TODAY = "today"
    LAST_7_DAYS = "last_7_days"
    LAST_30_DAYS = "last_30_days"


class KPIMetric(BaseModel):
    value: float
    growth_pct: float


class MerchantKPIsResponse(BaseModel):
    total_revenue: KPIMetric
    total_orders: KPIMetric
    conversion_rate: KPIMetric  # (Orders / Page Views) * 100
    active_customers: KPIMetric


class RevenueTrendDataPoint(BaseModel):
    timestamp: str
    revenue: float


class RevenueTrendResponse(BaseModel):
    trend: List[RevenueTrendDataPoint]


class FunnelResponse(BaseModel):
    visits: int
    add_to_cart: int
    checkout: int


class StorePortfolioRow(BaseModel):
    store_id: str
    store_name: str
    revenue: float
    orders: int
    conversion_rate: float
    status: str
    growth_pct: float


class StorePortfolioResponse(BaseModel):
    stores: List[StorePortfolioRow]
