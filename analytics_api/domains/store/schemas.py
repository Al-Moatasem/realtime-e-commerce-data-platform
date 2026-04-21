from typing import List

from pydantic import BaseModel


class TimeRangeMeta(BaseModel):
    start_date: str
    end_date: str


class StoreKPIMetric(BaseModel):
    value: float
    growth_pct: float


class StoreKPIsResponse(BaseModel):
    time_range: TimeRangeMeta
    revenue: StoreKPIMetric
    orders: StoreKPIMetric
    visitors: StoreKPIMetric
    aov: StoreKPIMetric  # Average Order Value


class TrafficConversionPoint(BaseModel):
    timestamp: str
    visits: int
    orders: int


class TrafficConversionResponse(BaseModel):
    data: List[TrafficConversionPoint]


class TopProductRow(BaseModel):
    product_id: str
    product_name: str
    revenue: float


class TopProductsResponse(BaseModel):
    products: List[TopProductRow]


class RecentOrderRow(BaseModel):
    order_id: str
    customer_name: str
    total_amount: float
    status: str


class RecentOrdersResponse(BaseModel):
    orders: List[RecentOrderRow]


class LowStockAlert(BaseModel):
    product_name: str
    quantity_left: int


class LowStockResponse(BaseModel):
    alerts: List[LowStockAlert]
