#!/usr/bin/env python3
"""Generate synthetic e-commerce datasets in JSON format."""
from __future__ import annotations

import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"

random.seed(2025)

FIRST_NAMES = [
    "Avery",
    "Brooke",
    "Cameron",
    "Dakota",
    "Elliot",
    "Finley",
    "Harper",
    "Jordan",
    "Kai",
    "Logan",
]

LAST_NAMES = [
    "Smith",
    "Johnson",
    "Reyes",
    "Hughes",
    "Patel",
    "Garcia",
    "Matsumoto",
    "Nakamura",
    "Silva",
    "Romero",
]

CITIES = [
    ("Seattle", "USA"),
    ("Vancouver", "Canada"),
    ("London", "UK"),
    ("Sydney", "Australia"),
    ("Berlin", "Germany"),
    ("Paris", "France"),
]

CATEGORIES = ["Electronics", "Home", "Outdoors", "Books", "Beauty", "Toys"]
REVIEW_COMMENTS = [
    "Great quality and fast shipping",
    "Product was okay, could be better",
    "Exceeded expectations",
    "Not worth the price",
    "Solid purchase overall",
    "Impressed with the durability",
]


@dataclass
class User:
    user_id: int
    name: str
    email: str
    location: str
    signup_date: str


@dataclass
class Product:
    product_id: int
    name: str
    category: str
    price: float
    stock: int


@dataclass
class Order:
    order_id: int
    user_id: int
    order_date: str
    total_amount: float


@dataclass
class OrderItem:
    item_id: int
    order_id: int
    product_id: int
    quantity: int
    price: float


@dataclass
class Review:
    review_id: int
    user_id: int
    product_id: int
    rating: int
    comment: str


def random_date(start: datetime, end: datetime) -> datetime:
    """Return a random datetime between start and end."""
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def generate_users(count: int) -> List[User]:
    """Create synthetic user records."""
    start = datetime(2019, 1, 1)
    end = datetime.now()
    users: List[User] = []
    for uid in range(1, count + 1):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, country = random.choice(CITIES)
        name = f"{first} {last}"
        email = f"{first.lower()}.{last.lower()}{uid}@example.com"
        location = f"{city}, {country}"
        signup_date = random_date(start, end).isoformat()
        users.append(User(uid, name, email, location, signup_date))
    return users


def generate_products(count: int) -> List[Product]:
    """Create synthetic product catalog entries."""
    products: List[Product] = []
    descriptors = ["Wireless", "Eco", "Pro", "Compact", "Ultra", "Smart"]
    nouns = ["Speaker", "Lamp", "Tent", "Cookbook", "Serum", "Drone", "Backpack"]
    for pid in range(1, count + 1):
        category = random.choice(CATEGORIES)
        name = f"{random.choice(descriptors)} {random.choice(nouns)}"
        price = round(random.uniform(10.0, 600.0), 2)
        stock = random.randint(10, 500)
        products.append(Product(pid, name, category, price, stock))
    return products


def generate_orders(count: int, users: List[User]) -> List[Order]:
    """Create orders referencing existing users."""
    start = datetime(2021, 1, 1)
    end = datetime.now()
    orders: List[Order] = []
    for oid in range(1, count + 1):
        user = random.choice(users)
        order_date = random_date(start, end).isoformat()
        orders.append(Order(oid, user.user_id, order_date, 0.0))
    return orders


def generate_order_items(count: int, orders: List[Order], products: List[Product]) -> List[OrderItem]:
    """Create order line items referencing orders and products."""
    items: List[OrderItem] = []
    for iid in range(1, count + 1):
        order = random.choice(orders)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        price = product.price
        items.append(OrderItem(iid, order.order_id, product.product_id, quantity, price))
    return items


def generate_reviews(count: int, users: List[User], products: List[Product]) -> List[Review]:
    """Create product reviews from users."""
    reviews: List[Review] = []
    for rid in range(1, count + 1):
        user = random.choice(users)
        product = random.choice(products)
        rating = random.randint(1, 5)
        comment = random.choice(REVIEW_COMMENTS)
        reviews.append(Review(rid, user.user_id, product.product_id, rating, comment))
    return reviews


def update_order_totals(orders: List[Order], items: List[OrderItem]) -> None:
    """Aggregate line items to compute order totals."""
    totals: dict[int, float] = {order.order_id: 0.0 for order in orders}
    for item in items:
        totals[item.order_id] += item.quantity * item.price
    for order in orders:
        order.total_amount = round(totals[order.order_id], 2)


def write_json(filename: str, payload: List[object]) -> None:
    """Serialize dataclasses to JSON."""
    path = DATA_DIR / filename
    path.write_text(json.dumps([asdict(item) for item in payload], indent=2))


def main() -> None:
    """Entrypoint to generate every dataset file."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    users = generate_users(50)
    products = generate_products(40)
    orders = generate_orders(100, users)
    order_items = generate_order_items(200, orders, products)
    update_order_totals(orders, order_items)
    reviews = generate_reviews(80, users, products)

    write_json("users.json", users)
    write_json("products.json", products)
    write_json("orders.json", orders)
    write_json("order_items.json", order_items)
    write_json("reviews.json", reviews)

    print(f"Wrote datasets to {DATA_DIR}")


if __name__ == "__main__":
    main()
