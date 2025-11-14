#!/usr/bin/env python3
"""Load CSV datasets into database/ecom.db."""
from __future__ import annotations

import csv
import sqlite3
from pathlib import Path
from typing import List

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DB_DIR = BASE_DIR / "database"
DB_PATH = DB_DIR / "ecom.db"

CSV_FILES = {
    "users": DATA_DIR / "users.csv",
    "products": DATA_DIR / "products.csv",
    "orders": DATA_DIR / "orders.csv",
    "order_items": DATA_DIR / "order_items.csv",
    "reviews": DATA_DIR / "reviews.csv",
}


def read_csv(path: Path) -> List[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing dataset: {path}")
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def create_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS reviews;
        DROP TABLE IF EXISTS order_items;
        DROP TABLE IF EXISTS orders;
        DROP TABLE IF EXISTS products;
        DROP TABLE IF EXISTS users;

        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            signup_date TEXT NOT NULL,
            country TEXT NOT NULL
        );

        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        );

        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            shipping_address TEXT NOT NULL,
            total_amount REAL NOT NULL
        );

        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL
        );

        CREATE TABLE reviews (
            review_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            rating INTEGER NOT NULL,
            comment TEXT NOT NULL
        );
        """
    )


def insert_all(conn: sqlite3.Connection) -> None:
    data = {name: read_csv(path) for name, path in CSV_FILES.items()}
    cur = conn.cursor()

    print("Loading users ...")
    cur.executemany(
        """
            INSERT INTO users (user_id, first_name, last_name, email, signup_date, country)
            VALUES (:user_id, :first_name, :last_name, :email, :signup_date, :country)
        """,
        data["users"],
    )

    print("Loading products ...")
    cur.executemany(
        """
            INSERT INTO products (product_id, name, category, price, stock)
            VALUES (:product_id, :name, :category, :price, :stock)
        """,
        data["products"],
    )

    print("Loading orders ...")
    cur.executemany(
        """
            INSERT INTO orders (order_id, user_id, order_date, status, shipping_address, total_amount)
            VALUES (:order_id, :user_id, :order_date, :status, :shipping_address, :total_amount)
        """,
        data["orders"],
    )

    print("Loading order items ...")
    cur.executemany(
        """
            INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, line_total)
            VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :line_total)
        """,
        data["order_items"],
    )

    print("Loading reviews ...")
    cur.executemany(
        """
            INSERT INTO reviews (review_id, user_id, product_id, rating, comment)
            VALUES (:review_id, :user_id, :product_id, :rating, :comment)
        """,
        data["reviews"],
    )

    conn.commit()
    print("SQLite database populated successfully.")


def main() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        create_tables(conn)
        insert_all(conn)
        print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    main()
