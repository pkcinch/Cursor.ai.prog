#!/usr/bin/env python3
"""Load JSON datasets into a SQLite database with proper schema."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
DB_PATH = ROOT_DIR / "database" / "ecom.db"


def load_json(filename: str) -> Sequence[dict]:
    """Read a JSON file from the data directory."""
    path = DATA_DIR / filename
    return json.loads(path.read_text())


def create_tables(conn: sqlite3.Connection) -> None:
    """Create normalized tables with foreign keys."""
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
            name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            location TEXT NOT NULL,
            signup_date TEXT NOT NULL
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
            total_amount REAL NOT NULL
        );

        CREATE TABLE order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
            product_id INTEGER NOT NULL REFERENCES products(product_id),
            quantity INTEGER NOT NULL,
            price REAL NOT NULL
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
    """Insert every dataset into its matching table."""
    cursor = conn.cursor()

    datasets = {
        "users": load_json("users.json"),
        "products": load_json("products.json"),
        "orders": load_json("orders.json"),
        "order_items": load_json("order_items.json"),
        "reviews": load_json("reviews.json"),
    }

    print("Inserting users ...")
    cursor.executemany(
        "INSERT INTO users(user_id, name, email, location, signup_date) VALUES (:user_id, :name, :email, :location, :signup_date)",
        datasets["users"],
    )

    print("Inserting products ...")
    cursor.executemany(
        "INSERT INTO products(product_id, name, category, price, stock) VALUES (:product_id, :name, :category, :price, :stock)",
        datasets["products"],
    )

    print("Inserting orders ...")
    cursor.executemany(
        "INSERT INTO orders(order_id, user_id, order_date, total_amount) VALUES (:order_id, :user_id, :order_date, :total_amount)",
        datasets["orders"],
    )

    print("Inserting order items ...")
    cursor.executemany(
        "INSERT INTO order_items(item_id, order_id, product_id, quantity, price) VALUES (:item_id, :order_id, :product_id, :quantity, :price)",
        datasets["order_items"],
    )

    print("Inserting reviews ...")
    cursor.executemany(
        "INSERT INTO reviews(review_id, user_id, product_id, rating, comment) VALUES (:review_id, :user_id, :product_id, :rating, :comment)",
        datasets["reviews"],
    )

    conn.commit()
    print("SQLite ingestion complete.")


def main() -> None:
    """Create the database and import all JSON data."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        create_tables(conn)
        insert_all(conn)
        print(f"Database ready at {DB_PATH}")


if __name__ == "__main__":
    main()
