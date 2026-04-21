import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Set, Tuple

from db.database import AsyncSessionLocal
from db.models import Customer, Product, Store
from schemas.clickstream import CartActionEvent, CheckoutStartedEvent, PageViewEvent
from services.kafka_service import kafka_producer_service
from sqlalchemy import select

logger = logging.getLogger(__name__)


class ProductSegment:
    def __init__(
        self,
        name: str,
        traffic_weight: int,
        conversion_rate: float,
        avg_items_per_cart: Tuple[int, int],
        description: str = "",
        is_active: bool = True,
    ):
        self.name = name
        self.description = description
        self.is_active = is_active
        self.traffic_weight = traffic_weight
        self.conversion_rate = conversion_rate
        self.avg_items_per_cart = avg_items_per_cart


class SessionSimulator:
    """
    Simplified Event Generator.
    Segments are sources for events.
    State is managed in memory as simple lists to allow easy progression lookups.
    """

    SEGMENTS = {
        "groceries_essential": ProductSegment(
            "groceries_essential", 80, 0.045, (8, 30), "Daily essentials."
        ),
        "fashion_apparel": ProductSegment(
            "fashion_apparel", 90, 0.025, (2, 5), "High traffic volume."
        ),
        "electronics_tech": ProductSegment(
            "electronics_tech", 60, 0.015, (1, 3), "Higher value items."
        ),
        "health_beauty": ProductSegment(
            "health_beauty", 70, 0.040, (3, 8), "Repeat purchases."
        ),
        "home_furniture": ProductSegment(
            "home_furniture", 40, 0.008, (1, 2), "Large ticket items."
        ),
        "pet_supplies": ProductSegment(
            "pet_supplies", 50, 0.035, (3, 10), "Consistent demand."
        ),
        "luxury_jewelry": ProductSegment(
            "luxury_jewelry", 15, 0.005, (1, 1), "Niche, high-value."
        ),
        "books_media": ProductSegment(
            "books_media", 45, 0.030, (2, 6), "Broad interest."
        ),
        "sports_outdoors": ProductSegment(
            "sports_outdoors", 40, 0.020, (1, 4), "Seasonal demand."
        ),
        "automotive_parts": ProductSegment(
            "automotive_parts", 25, 0.028, (2, 5), "Specific need-based."
        ),
    }

    def __init__(self):
        self.clickstream_topic = "storefront.clickstream"
        self.base_sessions_per_minute = 60
        self.allowed_event_types: Set[str] = {
            "page_view",
            "cart_action",
            "checkout_started",
        }

        # Flash Promo Rules state
        self.active_promos: Dict[str, Dict] = {}  # {store_id: {discount, timestamp}}
        self.promo_history: List[Dict] = []

        # Memory Pools for Progression
        # Each pool stores dicts with metadata needed for the NEXT step (view > cart > checkout)
        self.pool_views: List[Dict] = []
        self.pool_carts: List[Dict] = []

        self.store_cache: Dict[uuid.UUID, Dict] = {}
        self.store_ids: List[uuid.UUID] = []
        self.next_cache_refresh = datetime.now(timezone.utc)

    def configure(self, topic=None, rate=None, events=None):
        if topic:
            self.clickstream_topic = topic
        if rate is not None:
            self.base_sessions_per_minute = rate
        if events is not None:
            self.allowed_event_types = set(events)
        logger.info(
            f"Simulator configured: rate={self.base_sessions_per_minute}, events={self.allowed_event_types}"
        )

    async def refresh_cache(self):
        async with AsyncSessionLocal() as session:
            stores_res = await session.execute(
                select(Store).where(Store.status == "active")
            )
            stores = stores_res.scalars().all()
            self.store_cache.clear()
            self.store_ids.clear()
            for store in stores:
                prods_res = await session.execute(
                    select(Product).where(Product.store_id == store.store_id)
                )
                products = prods_res.scalars().all()
                if not products:
                    continue

                product_category_map = {}
                for p in products:
                    if p.category not in product_category_map:
                        product_category_map[p.category] = []
                    product_category_map[p.category].append(
                        {"id": p.product_id, "price": float(p.price)}
                    )

                customers_result = await session.execute(
                    select(Customer.customer_id).where(
                        Customer.merchant_id == store.merchant_id
                    )
                )
                self.store_cache[store.store_id] = {
                    "merchant_id": store.merchant_id,
                    "category_map": product_category_map,
                    "customers": customers_result.scalars().all(),
                }
                self.store_ids.append(store.store_id)

    async def _emit_page_views(self, count: int):
        """Step 1: Create initial interest."""
        for _ in range(count):
            store_id = random.choice(self.store_ids)
            data = self.store_cache[store_id]
            active_cats = [
                c
                for c in data["category_map"].keys()
                if self.SEGMENTS.get(c) and self.SEGMENTS[c].is_active
            ]
            if not active_cats:
                continue

            cat = random.choice(active_cats)
            product = random.choice(data["category_map"][cat])
            session_id = uuid.uuid4()

            # For Cart/Checkout, we REQUIRE a customer_id.
            # We assign it at the View stage to ensure consistency throughout the funnel.
            customer_id = (
                random.choice(data["customers"]) if data["customers"] else None
            )

            if "page_view" in self.allowed_event_types:
                ev = PageViewEvent(
                    session_id=session_id,
                    store_id=store_id,
                    customer_id=customer_id,
                    product_id=product["id"],
                )
                await kafka_producer_service.produce_message(
                    self.clickstream_topic, ev, session_id
                )

            # Progression safety: only add to pools if we have a valid customer_id
            # required by downstream Cart/Checkout schemas
            if customer_id:
                self.pool_views.append(
                    {
                        "session_id": session_id,
                        "store_id": store_id,
                        "customer_id": customer_id,
                        "product_id": product["id"],
                        "price": product["price"],
                        "category": cat,
                    }
                )

    async def _emit_cart_actions(self):
        """Step 2: Convert Views to Carts based on Rate."""
        to_process = self.pool_views[:]
        self.pool_views.clear()

        for view in to_process:
            segment = self.SEGMENTS[view["category"]]
            # Simple Rate Check
            if random.random() < segment.conversion_rate:
                if "cart_action" in self.allowed_event_types:
                    qty = random.randint(1, 3)
                    ev = CartActionEvent(
                        session_id=view["session_id"],
                        store_id=view["store_id"],
                        customer_id=view["customer_id"],
                        product_id=view["product_id"],
                        action="add",
                        quantity=qty,
                        unit_price=view["price"],
                    )
                    await kafka_producer_service.produce_message(
                        self.clickstream_topic, ev, view["customer_id"]
                    )

                # Progress to Cart Pool
                self.pool_carts.append(view)

    async def _emit_checkouts(self):
        """Step 3: Convert Carts to Checkouts."""
        to_process = self.pool_carts[:]
        self.pool_carts.clear()

        for cart in to_process:
            # 80% fixed progression from cart to checkout for simplicity
            if random.random() < 0.8:
                if "checkout_started" in self.allowed_event_types:
                    ev = CheckoutStartedEvent(
                        session_id=cart["session_id"],
                        store_id=cart["store_id"],
                        customer_id=cart["customer_id"],
                        cart_item_count=1,
                        expected_total=cart["price"],
                    )
                    await kafka_producer_service.produce_message(
                        self.clickstream_topic, ev, cart["customer_id"]
                    )

                    # Also persist to DB via sim_service
                    from data_faker.services.sim_service import (
                        persist_order_from_checkout,
                    )

                    await persist_order_from_checkout(
                        ev,
                        [(cart["product_id"], 1, cart["price"])],
                        self.store_cache[cart["store_id"]]["merchant_id"],
                    )

    async def simulate_sessions_task(self):
        if datetime.now(timezone.utc) >= self.next_cache_refresh:
            await self.refresh_cache()
            self.next_cache_refresh = datetime.now(timezone.utc) + timedelta(minutes=5)

        if not self.store_ids:
            return

        # sessions per tick (5s interval)
        spawn_count = max(1, int(self.base_sessions_per_minute / 12))

        # 1. Generate new views
        await self._emit_page_views(spawn_count)

        # 2. Process pools
        await self._emit_cart_actions()
        await self._emit_checkouts()

        # Keep pools from growing infinitely if types are disabled
        if len(self.pool_views) > 1000:
            self.pool_views = self.pool_views[-500:]
        if len(self.pool_carts) > 1000:
            self.pool_carts = self.pool_carts[-500:]


session_simulator = SessionSimulator()
