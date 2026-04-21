import logging
import random

from db.database import AsyncSessionLocal
from db.models import Order, OrderStatus
from schemas.logistics import RefundIssuedEvent, ShipmentStatusChangedEvent
from services.kafka_service import kafka_producer_service
from sqlalchemy import select

logger = logging.getLogger(__name__)

# Courier options for realistic simulation
COURIERS = ["Aramex", "SMSA", "DHL", "FedEx", "Naqel"]
REFUND_REASONS = ["damaged", "customer_regret"]


async def order_fulfillment_loop():
    """
    Background task that fetches orders in various states and randomly
    progresses their fulfillment status, emitting Logistics events to Kafka.
    """
    topic = "logistics.events"

    async with AsyncSessionLocal() as session:
        try:
            # --- 1. Progress PENDING -> PROCESSING ---
            # Fetch orders that have been pending for at least a short time (e.g., a few seconds or minutes)
            stmt = select(Order).where(Order.status == OrderStatus.PENDING).limit(100)
            pending_orders = (await session.execute(stmt)).scalars().all()

            for order in pending_orders:
                # 80% chance to immediately start processing a pending order
                if random.random() < 0.8:
                    order.status = OrderStatus.PROCESSING

            # --- 2. Progress PROCESSING -> SHIPPED ---
            # In a real app, this takes days. We can simulate it by random chance.
            stmt = (
                select(Order).where(Order.status == OrderStatus.PROCESSING).limit(100)
            )
            processing_orders = (await session.execute(stmt)).scalars().all()

            for order in processing_orders:
                if random.random() < 0.3:  # 30% chance per interval to dispatch
                    order.status = OrderStatus.SHIPPED

                    # Emit Kafka Event
                    event = ShipmentStatusChangedEvent(
                        order_id=order.order_id,
                        courier_name=random.choice(COURIERS),
                        status="shipped",
                        tracking_number=f"TRK{random.randint(10000000, 99999999)}{random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}",
                    )
                    await kafka_producer_service.produce_message(topic, event)

            # --- 3. Progress SHIPPED -> DELIVERED ---
            stmt = select(Order).where(Order.status == OrderStatus.SHIPPED).limit(100)
            shipped_orders = (await session.execute(stmt)).scalars().all()

            for order in shipped_orders:
                if random.random() < 0.2:  # 20% chance to reach destination
                    order.status = OrderStatus.DELIVERED

                    # We could emit another ShipmentStatusChangedEvent if we wanted,
                    # but the plan only mandates "shipped", "partially_shipped", "returned" for that event.

            # --- 4. Process REFUNDS (Simulating returns on delivered orders) ---
            # We want to randomly transition ~5% of DELIVERED orders to REFUNDED status.
            stmt = select(Order).where(Order.status == OrderStatus.DELIVERED).limit(50)
            delivered_orders = (await session.execute(stmt)).scalars().all()

            for order in delivered_orders:
                if random.random() < 0.05:  # 5% churn/return rate
                    order.status = OrderStatus.REFUNDED

                    # Emit Shipment event indicating return transit (optional step for visibility)
                    shipment_event = ShipmentStatusChangedEvent(
                        order_id=order.order_id,
                        courier_name=random.choice(COURIERS),
                        status="returned",
                        tracking_number=f"RET{random.randint(10000000, 99999999)}",
                    )
                    await kafka_producer_service.produce_message(topic, shipment_event)

                    # Emit strict RefundIssuedEvent
                    refund_event = RefundIssuedEvent(
                        order_id=order.order_id,
                        refund_amount=float(order.total_amount),
                        reason=random.choice(REFUND_REASONS),
                    )
                    await kafka_producer_service.produce_message(topic, refund_event)

            # Commit all changes to PostgreSQL.
            # Debezium will detect this and automatically scrape the UPDATES into its own Kafka CDC topics.
            await session.commit()

            total_processed = (
                len(pending_orders)
                + len(processing_orders)
                + len(shipped_orders)
                + len(delivered_orders)
            )
            if total_processed > 0:
                logger.info(
                    f"Fulfillment Loop Processed {total_processed} mixed state orders."
                )

        except Exception as e:
            await session.rollback()
            logger.error(f"Error during fulfillment loop step: {e}")
