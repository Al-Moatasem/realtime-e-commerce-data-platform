from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

class FlashPromoRuleEvent(BaseModel):
    event_name: str = "flash_promo_broadcast"
    rule_id: UUID
    store_id: UUID
    discount_percentage: float = Field(..., gt=0, le=100)
    active_from: datetime
    active_until: datetime

    model_config = ConfigDict(ser_json_timedelta="iso8601")
