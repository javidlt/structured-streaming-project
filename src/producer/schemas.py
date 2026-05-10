"""Value pools, dataclasses, and synthetic-data helpers for the streaming producer.

These mirror the Part-I batch generator field names so downstream Cypher / Spark
schemas stay aligned. Streaming uses smaller reference-data cardinalities than the
batch version (1000 customers / 200 merchants) so the resulting Neo4j graph keeps
relationship density high enough to be visually interesting.
"""
from __future__ import annotations

import random
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

COUNTRIES: tuple[str, ...] = ("Mexico", "USA", "Canada", "Brazil", "Spain")
CITIES: tuple[str, ...] = (
    "Guadalajara", "Zapopan", "Monterrey", "CDMX", "Puebla",
    "Toronto", "Madrid", "Sao Paulo",
)
CATEGORIES: tuple[str, ...] = (
    "electronics", "clothing", "groceries", "restaurants",
    "travel", "health", "education", "entertainment",
)
STATUSES: tuple[str, ...] = ("approved", "declined", "pending")
PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card", "debit_card", "bank_transfer", "cash", "digital_wallet",
)
CURRENCIES: tuple[str, ...] = ("MXN", "USD", "CAD", "BRL", "EUR")
DEVICE_TYPES: tuple[str, ...] = ("mobile", "desktop", "pos_terminal", "atm")


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: int
    customer_name: str
    email: str
    customer_country: str
    customer_city: str
    age: int
    registration_date: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Merchant:
    merchant_id: int
    merchant_name: str
    category: str
    merchant_country: str
    merchant_city: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Transaction:
    transaction_id: int
    customer_id: int
    merchant_id: int
    transaction_date: str
    amount: float
    currency: str
    transaction_country: str
    transaction_city: str
    payment_method: str
    status: str
    device_type: str
    description: str

    def to_dict(self) -> dict:
        return asdict(self)


def build_customers(n: int, seed: int = 42) -> list[Customer]:
    rng = random.Random(seed)
    base_date = datetime(2018, 1, 1, tzinfo=timezone.utc)
    customers: list[Customer] = []
    for cid in range(1, n + 1):
        reg_offset_days = rng.randint(0, 365 * 6)
        reg_date = base_date.fromordinal(base_date.toordinal() + reg_offset_days)
        customers.append(
            Customer(
                customer_id=cid,
                customer_name=f"Customer_{cid}",
                email=f"customer_{cid}@example.com",
                customer_country=rng.choice(COUNTRIES),
                customer_city=rng.choice(CITIES),
                age=rng.randint(18, 80),
                registration_date=reg_date.date().isoformat(),
            )
        )
    return customers


def build_merchants(n: int, seed: int = 1337) -> list[Merchant]:
    rng = random.Random(seed)
    merchants: list[Merchant] = []
    for mid in range(1, n + 1):
        merchants.append(
            Merchant(
                merchant_id=mid,
                merchant_name=f"Merchant_{mid}",
                category=rng.choice(CATEGORIES),
                merchant_country=rng.choice(COUNTRIES),
                merchant_city=rng.choice(CITIES),
            )
        )
    return merchants


def generate_transaction(transaction_id: int, num_customers: int, num_merchants: int,
                         rng: random.Random) -> Transaction:
    now_iso = datetime.now(timezone.utc).isoformat()
    amount = round(rng.uniform(10.0, 50_000.0), 2)
    return Transaction(
        transaction_id=transaction_id,
        customer_id=rng.randint(1, num_customers),
        merchant_id=rng.randint(1, num_merchants),
        transaction_date=now_iso,
        amount=amount,
        currency=rng.choice(CURRENCIES),
        transaction_country=rng.choice(COUNTRIES),
        transaction_city=rng.choice(CITIES),
        payment_method=rng.choice(PAYMENT_METHODS),
        status=rng.choice(STATUSES),
        device_type=rng.choice(DEVICE_TYPES),
        description=f"synthetic_transaction_description_{transaction_id}_streaming_big_data_project",
    )
