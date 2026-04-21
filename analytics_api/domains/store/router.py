from core.utils import TimePeriod
from domains.store.schemas import (
    LowStockResponse,
    RecentOrdersResponse,
    StoreKPIsResponse,
    TopProductsResponse,
    TrafficConversionResponse,
)
from domains.store.service import StoreService
from fastapi import APIRouter, Depends, Query

router = APIRouter(prefix="/api/v1/stores", tags=["Stores"])


def get_store_service():
    return StoreService()


@router.get("/{store_id}/kpis", response_model=StoreKPIsResponse)
def get_store_kpis(
    store_id: str,
    period: TimePeriod = Query(TimePeriod.LAST_7_DAYS),
    service: StoreService = Depends(get_store_service),
):
    return service.get_kpis(store_id, period)


@router.get("/{store_id}/traffic-conversion", response_model=TrafficConversionResponse)
def get_traffic_conversion(
    store_id: str,
    period: TimePeriod = Query(TimePeriod.LAST_7_DAYS),
    service: StoreService = Depends(get_store_service),
):
    return service.get_traffic_conversion(store_id, period)


@router.get("/{store_id}/top-products", response_model=TopProductsResponse)
def get_top_products(
    store_id: str,
    period: TimePeriod = Query(TimePeriod.LAST_7_DAYS),
    service: StoreService = Depends(get_store_service),
):
    return service.get_top_products(store_id, period)


@router.get("/{store_id}/recent-orders", response_model=RecentOrdersResponse)
def get_recent_orders(
    store_id: str, service: StoreService = Depends(get_store_service)
):
    # Notice we don't pass the period filter here, UI usually expects absolute latest
    return service.get_recent_orders(store_id)


@router.get("/{store_id}/low-stock", response_model=LowStockResponse)
def get_low_stock(store_id: str, service: StoreService = Depends(get_store_service)):
    return service.get_low_stock_alerts(store_id)
