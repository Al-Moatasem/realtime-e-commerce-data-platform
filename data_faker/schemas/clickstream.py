from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ClickstreamBase(BaseModel):
    session_id: UUID
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    store_id: UUID
    customer_id: Optional[UUID] = None

    model_config = ConfigDict(ser_json_timedelta="iso8601")


class PageViewEvent(ClickstreamBase):
    event_name: str = "page_view"
    product_id: Optional[UUID] = None


class CartActionEvent(ClickstreamBase):
    event_name: str = "cart_action"
    customer_id: UUID
    product_id: UUID
    action: str = Field(..., pattern="^(add|remove)$")
    quantity: int
    unit_price: float


class CheckoutStartedEvent(ClickstreamBase):
    event_name: str = "checkout_started"
    customer_id: UUID
    cart_item_count: int
    expected_total: float
