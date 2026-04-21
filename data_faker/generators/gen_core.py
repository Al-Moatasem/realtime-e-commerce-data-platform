import random
from typing import Any, Dict
from uuid import uuid4

from core.config import settings
from db.models import StoreStatus, SubscriptionStatus, SubscriptionTier
from faker import Faker

fake = Faker()


class MerchantGenerator:
    @staticmethod
    def generate_merchant() -> Dict[str, Any]:
        return {
            "merchant_id": uuid4(),
            "company_name": f"{fake.company()} {fake.company_suffix()}",
        }

    @staticmethod
    def generate_customer(merchant_id: str) -> Dict[str, Any]:
        return {
            "customer_id": uuid4(),
            "merchant_id": merchant_id,
            "email": fake.email(),
            "hashed_password": fake.password(length=64),
            "shipping_address": fake.address().replace("\n", ", "),
        }

    @staticmethod
    def generate_subscription(merchant_id: str) -> Dict[str, Any]:
        return {
            "subscription_id": uuid4(),
            "merchant_id": merchant_id,
            "plan_tier": random.choice(list(SubscriptionTier)),
            "status": random.choice(list(SubscriptionStatus)),
            "billing_cycle": random.choice(["monthly", "annually"]),
        }

    @staticmethod
    def generate_store(merchant_id: str) -> Dict[str, Any]:
        region = random.choice(["US", "EU", "ASIA"])
        return {
            "store_id": uuid4(),
            "merchant_id": merchant_id,
            "name": f"{fake.company()} {fake.word().capitalize()}",
            "domain_name": f"www.{fake.domain_name()}",
            "region": region,
            "status": random.choice(list(StoreStatus)),
        }


class ProductGenerator:
    @staticmethod
    def generate_product(
        merchant_id: str,
        store_id: str,
        category: str = None,
    ) -> Dict[str, Any]:
        return {
            "product_id": uuid4(),
            "merchant_id": merchant_id,
            "store_id": store_id,
            "name": f"{fake.color_name()} {fake.word().capitalize()} {fake.word().capitalize()}",
            "sku": f"{fake.bothify(text='??-####-####').upper()}",
            "category": category or random.choice(settings.PRODUCT_CATEGORIES),
            "price": fake.pydecimal(
                left_digits=3,
                right_digits=2,
                positive=True,
                min_value=10,
                max_value=999,
            ),
        }
