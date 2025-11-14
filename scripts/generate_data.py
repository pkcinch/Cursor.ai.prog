#!/usr/bin/env python3
"""Generate synthetic e-commerce CSV datasets under project14/data."""
from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

random.seed(2025)

FIRST_NAMES = ["Avery", "Brooke", "Cameron", "Dakota", "Elliot", "Finley", "Harper", "Jordan", "Kai", "Logan"]
LAST_NAMES = ["Smith", "Johnson", "Lee", "Patel", "Garcia", "Brown", "Martinez", "Lopez", "Kim", "Reyes"]
CITIES = [
    ("Seattle", "United States"),
    ("Toronto", "Canada"),
    ("London", "United Kingdom"),
    ("Berlin", "Germany"),
    ("Paris", "France"),
    ("Sydney", "Australia"),
]
PRODUCT_CATEGORIES = ["Electronics", "Home", "Outdoors", "Books", "Beauty", "Toys"]
ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]
REVIEW_COMMENTS = [
    "Fantastic quality and fast shipping",
    "Product was okay, could be better",
    "Exceeded expectations",
    "Not worth the price",
    "Solid purchase overall",
    "Impressed with the durability",
]

def random_date(start: datetime, end: datetime) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))


def write_csv(path: Path, headers: Iterable[str], rows: Iterable[Iterable[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)


def generate() -> None:
    now = datetime.now()
    users = []
    for user_id in range(1, 51):
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        city, country = random.choice(CITIES)
        users.append(
            (
                user_id,
                first,
                last,
                f"{first.lower()}.{last.lower()}{user_id}@example.com",
                random_date(datetime(2019, 1, 1), now).isoformat(),
                country,
            )
        )

    products = []
    adjectives = ["Wireless", "Smart", "Eco", "Ultra", "Compact", "Pro"]
    nouns = ["Speaker", "Lamp", "Backpack", "Tent", "Cookbook", "Serum", "Drone", "Headset"]
    price_lookup = {}
    for product_id in range(1, 41):
        name = f"{random.choice(adjectives)} {random.choice(nouns)}"
        category = random.choice(PRODUCT_CATEGORIES)
        price = round(random.uniform(10, 600), 2)
        stock = random.randint(25, 500)
        products.append((product_id, name, category, price, stock))
        price_lookup[product_id] = price

    orders = []
    for order_id in range(1, 101):
        user_id = random.randint(1, 50)
        order_date = random_date(datetime(2021, 1, 1), now).isoformat()
        status = random.choice(ORDER_STATUSES)
        address = f"{random.randint(100, 9999)} {random.choice(['Oak', 'Pine', 'Cedar', 'Maple'])} St"
        orders.append((order_id, user_id, order_date, status, address, 0.0))

    order_items = []
    for order_item_id in range(1, 201):
        order = random.randint(1, 100)
        product = random.randint(1, 40)
        quantity = random.randint(1, 5)
        unit_price = price_lookup[product]
        line_total = round(quantity * unit_price, 2)
        order_items.append((order_item_id, order, product, quantity, unit_price, line_total))

    # Update order totals
    totals = {order_id: 0.0 for order_id in range(1, 101)}
    for _, order_id, _, _, _, line_total in order_items:
        totals[order_id] += line_total
    orders = [(*order[:5], round(totals[order[0]], 2)) for order in orders]

    reviews = []
    for review_id in range(1, 81):
        user_id = random.randint(1, 50)
        product_id = random.randint(1, 40)
        rating = random.randint(1, 5)
        comment = random.choice(REVIEW_COMMENTS)
        reviews.append((review_id, user_id, product_id, rating, comment))

    write_csv(DATA_DIR / "users.csv", ["user_id", "first_name", "last_name", "email", "signup_date", "country"], users)
    write_csv(DATA_DIR / "products.csv", ["product_id", "name", "category", "price", "stock"], products)
    write_csv(
        DATA_DIR / "orders.csv",
        ["order_id", "user_id", "order_date", "status", "shipping_address", "total_amount"],
        orders,
    )
    write_csv(
        DATA_DIR / "order_items.csv",
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "line_total"],
        order_items,
    )
    write_csv(DATA_DIR / "reviews.csv", ["review_id", "user_id", "product_id", "rating", "comment"], reviews)

    print(f"Synthetic CSV datasets written to {DATA_DIR}")


def main() -> None:
    generate()


if __name__ == "__main__":
    main()
