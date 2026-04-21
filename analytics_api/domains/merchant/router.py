from domains.merchant.schemas import (
    FunnelResponse,
    MerchantKPIsResponse,
    RevenueTrendResponse,
    StorePortfolioResponse,
    TimePeriod,
)
from domains.merchant.service import MerchantService
from fastapi import APIRouter, Depends, Query

router = APIRouter(prefix="/api/v1/merchants", tags=["Merchants"])


def get_merchant_service():
    return MerchantService()


@router.get("/{merchant_id}/kpis", response_model=MerchantKPIsResponse)
def get_merchant_kpis(
    merchant_id: str,
    period: TimePeriod = Query(
        TimePeriod.LAST_30_DAYS, description="Time period filter"
    ),
    service: MerchantService = Depends(get_merchant_service),
):
    return service.get_kpis(merchant_id, period)


@router.get("/{merchant_id}/revenue-trend", response_model=RevenueTrendResponse)
def get_revenue_trend(
    merchant_id: str,
    period: TimePeriod = Query(TimePeriod.LAST_30_DAYS),
    service: MerchantService = Depends(get_merchant_service),
):
    return service.get_revenue_trend(merchant_id, period)


@router.get("/{merchant_id}/funnel", response_model=FunnelResponse)
def get_funnel(
    merchant_id: str,
    period: TimePeriod = Query(TimePeriod.LAST_30_DAYS),
    service: MerchantService = Depends(get_merchant_service),
):
    return service.get_funnel(merchant_id, period)


@router.get("/{merchant_id}/stores", response_model=StorePortfolioResponse)
def get_store_portfolio(
    merchant_id: str,
    period: TimePeriod = Query(TimePeriod.LAST_30_DAYS),
    service: MerchantService = Depends(get_merchant_service),
):
    return service.get_store_portfolio(merchant_id, period)
