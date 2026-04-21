from datetime import datetime, timezone
from uuid import UUID
from pydantic import BaseModel, ConfigDict, Field

class LogisticsBase(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    model_config = ConfigDict(ser_json_timedelta="iso8601")

class ShipmentStatusChangedEvent(LogisticsBase):
    order_id: UUID
    courier_name: str
    status: str = Field(..., pattern="^(shipped|partially_shipped|returned)$")
    tracking_number: str

class RefundIssuedEvent(LogisticsBase):
    order_id: UUID
    refund_amount: float
    reason: str = Field(..., pattern="^(damaged|customer_regret)$")
