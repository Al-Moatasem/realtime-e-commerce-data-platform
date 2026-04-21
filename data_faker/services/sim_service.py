import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
from uuid import UUID

from db.database import AsyncSessionLocal
from db.models import (
    Customer,
    Merchant,
    Order,
    OrderLine,
    OrderStatus,
    Product,
    Store,
    Subscription,
)
from generators.gen_core import MerchantGenerator, ProductGenerator
from schemas.clickstream import CartActionEvent, CheckoutStartedEvent, PageViewEvent
from sqlalchemy import select

from services.kafka_service import kafka_producer_service

SIM_HISTORY_FILE = "simulated_merchants.json"

logger = logging.getLogger(__name__)


async def onboard_merchant_task():
    """Task to onboard a new merchant with 1-3 stores and their initial subscription and products."""
    async with AsyncSessionLocal() as session:
        try:
            # Generate Merchant
            merchant_data = MerchantGenerator.generate_merchant()
            merchant = Merchant(**merchant_data)
            session.add(merchant)
            await session.commit()
            await session.refresh(merchant)

            # Generate Subscription
            sub_data = MerchantGenerator.generate_subscription(merchant.merchant_id)
            subscription = Subscription(**sub_data)
            session.add(subscription)

            # Generate 5-15 Customers for this merchant
            num_customers = random.randint(5, 15)
            for _ in range(num_customers):
                customer_data = MerchantGenerator.generate_customer(
                    merchant.merchant_id
                )
                customer = Customer(**customer_data)
                session.add(customer)

            # Generate 1-5 Stores
            num_stores = random.randint(1, 5)
            for _ in range(num_stores):
                store_data = MerchantGenerator.generate_store(merchant.merchant_id)
                store = Store(**store_data)
                session.add(store)
                await session.flush()  # Flush to get store_id

                # Generate 10-50 Products for each Store
                num_products = random.randint(10, 50)
                for _ in range(num_products):
                    product_data = ProductGenerator.generate_product(
                        merchant.merchant_id, store.store_id
                    )
                    product = Product(**product_data)
                    session.add(product)

            await session.commit()
            logger.info(
                f"Onboarded merchant '{merchant.company_name}' with {num_stores} stores."
            )
        except Exception as e:
            await session.rollback()
            logger.error(f"Failed to onboard merchant: {e}")


async def update_products_task():
    """Task to update random product prices (simulating updates)."""
    async with AsyncSessionLocal() as session:
        try:
            # Select random products
            stmt = select(Product).order_by(random.random()).limit(10)
            result = await session.execute(stmt)
            products = result.scalars().all()

            for product in products:
                # Update price by +/- 5-15%
                change_factor = random.uniform(0.85, 1.15)
                product.price = float(product.price) * change_factor

            await session.commit()
            logger.info(f"Updated prices for {len(products)} products.")
        except Exception as e:
            await session.rollback()
            logger.error(f"Failed to update product prices: {e}")


async def persist_order_from_checkout(
    event: CheckoutStartedEvent,
    cart_items: List[Tuple[UUID, int, float]],
    merchant_id: UUID,
):
    """
    Takes a successful Checkout event and saves it to the OLTP Database.
    cart_items tuple structure is: (product_id, quantity, unit_price)
    """
    async with AsyncSessionLocal() as session:
        try:
            # Create the main Order record
            new_order = Order(
                merchant_id=merchant_id,
                store_id=event.store_id,
                customer_id=event.customer_id,
                status=OrderStatus.PENDING,
                total_amount=event.expected_total,
            )
            session.add(new_order)
            await session.flush()  # Get the new_order.order_id

            # Create the OrderLines
            for product_id, qty, price in cart_items:
                line = OrderLine(
                    order_id=new_order.order_id,
                    product_id=product_id,
                    quantity=qty,
                    unit_price=price,
                )
                session.add(line)

            await session.commit()
            logger.debug(f"Persisted order for session {event.session_id}")

        except Exception as e:
            await session.rollback()
            logger.error(f"Failed to persist checkout order: {e}")


async def run_bulk_init_task(
    merchants_count: int, stores_count: int, products_count: int, customers_count: int
):
    """Efficiently seeds the database with a large volume of data using bulk inserts."""
    async with AsyncSessionLocal() as session:
        try:
            # 1. Generate Merchants
            merchants = [
                Merchant(**MerchantGenerator.generate_merchant())
                for _ in range(merchants_count)
            ]
            session.add_all(merchants)
            await session.flush()
            merchant_ids = [m.merchant_id for m in merchants]

            # 2. Generate Subscriptions & Customers
            subscriptions = []
            customers = []
            for mid in merchant_ids:
                subscriptions.append(
                    Subscription(**MerchantGenerator.generate_subscription(mid))
                )
                # Distribute customers across merchants
                cust_per_merch = max(1, customers_count // merchants_count)
                for _ in range(cust_per_merch):
                    customers.append(
                        Customer(**MerchantGenerator.generate_customer(mid))
                    )

            session.add_all(subscriptions)
            session.add_all(customers)

            # 3. Generate Stores
            stores = []
            for _ in range(stores_count):
                mid = random.choice(merchant_ids)
                stores.append(Store(**MerchantGenerator.generate_store(mid)))

            session.add_all(stores)
            await session.flush()

            store_ids_map = {s.store_id: s.merchant_id for s in stores}
            store_list = list(store_ids_map.keys())

            # 4. Generate Products
            products = []
            for _ in range(products_count):
                sid = random.choice(store_list)
                mid = store_ids_map[sid]
                products.append(Product(**ProductGenerator.generate_product(mid, sid)))

            session.add_all(products)
            await session.commit()
            return True
        except Exception as e:
            await session.rollback()
            logger.error(f"Bulk init failed: {e}")
            raise


def _add_to_sim_history(merchant_id: str, name: str):
    history = []
    if os.path.exists(SIM_HISTORY_FILE):
        try:
            with open(SIM_HISTORY_FILE, "r") as f:
                history = json.load(f)
        except:
            pass

    # Remove if exists to update last_run
    history = [m for m in history if m["id"] != merchant_id]
    history.insert(
        0,
        {
            "id": merchant_id,
            "name": name,
            "status": "Simulated",
            "last_run": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M"),
        },
    )

    with open(SIM_HISTORY_FILE, "w") as f:
        json.dump(history[:50], f)  # Keep last 50


async def run_merchant_lifecycle_task(
    backfill_days: int = 33, conversion_rate: float = 0.15
):
    """
    1. Onboard a fresh merchant (reuse current logic)
    2. Backfill N days of history (KAFKA + DB)
    3. Ensure they are part of the active live simulation
    """
    async with AsyncSessionLocal() as session:
        # Onboarding logic
        merchant_data = MerchantGenerator.generate_merchant()
        merchant = Merchant(**merchant_data)
        session.add(merchant)
        await session.commit()
        await session.refresh(merchant)

        sub = Subscription(
            **MerchantGenerator.generate_subscription(merchant.merchant_id)
        )
        session.add(sub)

        customers = []
        for _ in range(20):
            c = Customer(**MerchantGenerator.generate_customer(merchant.merchant_id))
            customers.append(c)
            session.add(c)

        stores = []
        for _ in range(random.randint(1, 4)):
            s = Store(**MerchantGenerator.generate_store(merchant.merchant_id))
            stores.append(s)
            session.add(s)
            await session.flush()

            prods = []
            for _ in range(random.randint(20, 50)):
                prod = Product(
                    **ProductGenerator.generate_product(
                        merchant.merchant_id, s.store_id
                    )
                )
                prods.append(prod)
                session.add(prod)
            s._products = prods  # Attach temporarily for backfill loop

        await session.commit()
        logger.info(
            f"Lifecycle: Created merchant {merchant.company_name}. Starting backfill ({backfill_days} days)..."
        )

        _add_to_sim_history(str(merchant.merchant_id), merchant.company_name)

        # Step 2: Historical Backfill
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=backfill_days)

        current_time = start_date
        while current_time < end_date:
            # Simulate growing traffic factor
            daily_growth_factor = (current_time - start_date).days / max(
                1, backfill_days
            )
            num_sessions = random.randint(2, 5) + int(5 * daily_growth_factor)

            for _ in range(num_sessions):
                store = random.choice(stores)
                session_id = uuid.uuid4()
                customer = random.choice(customers)

                # 1. Page View
                view = PageViewEvent(
                    session_id=session_id,
                    store_id=store.store_id,
                    customer_id=customer.customer_id,
                    timestamp=current_time,
                )
                await kafka_producer_service.produce_message(
                    "storefront.clickstream", view, str(store.store_id)
                )

                # 2. Intent & Conversion Funnel
                if random.random() < 0.30:  # 30% of visitors add to cart
                    # 2.1 Select items for this session ONCE to ensure parity
                    session_items = []
                    for _ in range(random.randint(1, 3)):
                        p = random.choice(store._products)
                        qty = random.randint(1, 2)
                        session_items.append({"prod": p, "qty": qty})

                    # 2.2 Emit Cart Actions (ADD) for each item
                    for item in session_items:
                        p = item["prod"]
                        cart_add = CartActionEvent(
                            session_id=session_id,
                            store_id=store.store_id,
                            customer_id=customer.customer_id,
                            product_id=p.product_id,
                            action="add",
                            quantity=item["qty"],
                            unit_price=float(p.price),
                            timestamp=current_time + timedelta(minutes=1),
                        )
                        await kafka_producer_service.produce_message(
                            "storefront.clickstream", cart_add, str(store.store_id)
                        )

                    # 2.3 Conversion Filter: Proceed to checkout?
                    if random.random() < (conversion_rate / 0.30):
                        total_amt = sum(
                            float(i["prod"].price) * i["qty"] for i in session_items
                        )

                        checkout = CheckoutStartedEvent(
                            session_id=session_id,
                            store_id=store.store_id,
                            customer_id=customer.customer_id,
                            cart_item_count=len(session_items),
                            expected_total=total_amt,
                            timestamp=current_time + timedelta(minutes=2),
                        )
                        await kafka_producer_service.produce_message(
                            "storefront.clickstream", checkout, str(store.store_id)
                        )

                        # 2.4 Persist EXACT same items to Database
                        order_time = (current_time + timedelta(minutes=2)).replace(
                            tzinfo=None
                        )
                        new_order = Order(
                            merchant_id=merchant.merchant_id,
                            store_id=store.store_id,
                            customer_id=customer.customer_id,
                            total_amount=total_amt,
                            status=OrderStatus.DELIVERED,
                            created_at=order_time,
                        )
                        session.add(new_order)
                        await session.flush()

                        for item in session_items:
                            p = item["prod"]
                            line = OrderLine(
                                order_id=new_order.order_id,
                                product_id=p.product_id,
                                quantity=item["qty"],
                                unit_price=float(p.price),
                            )
                            session.add(line)

            await session.commit()
            current_time += timedelta(hours=random.randint(2, 6))

        logger.info(f"Lifecycle: Backfill for {merchant.company_name} complete.")
        return {"merchant_id": str(merchant.merchant_id), "name": merchant.company_name}
