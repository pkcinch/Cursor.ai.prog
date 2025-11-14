#!/usr/bin/env python3
"""Example ad-hoc queries using the SQLite ecommerce database."""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable, Sequence

BASE = Path(__file__).resolve().parents[1]
DB_PATH = BASE / "database" / "ecom.db"


def query(sql: str, params: Sequence[object] | None = None) -> list[tuple]:
    if not DB_PATH.exists():
        raise SystemExit(f"Database not found: {DB_PATH}")
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(sql, params or tuple())
        return cur.fetchall()


def print_rows(title: str, rows: Iterable[tuple]) -> None:
    print(f"\n--- {title} ---")
    for row in rows:
        print(row)


def main() -> None:
    rows = query(
        """
        SELECT
            o.order_id,
            u.first_name || ' ' || u.last_name AS customer,
            p.name AS product_name,
            oi.quantity,
            oi.unit_price,
            oi.line_total
        FROM orders AS o
        JOIN users AS u ON u.user_id = o.user_id
        JOIN order_items AS oi ON oi.order_id = o.order_id
        JOIN products AS p ON p.product_id = oi.product_id
        ORDER BY o.order_id
        LIMIT 10;
        """
    )
    print_rows("Top 10 order line items with user & product", rows)

    rows = query(
        """
        SELECT
            p.name,
            SUM(oi.quantity * oi.unit_price) AS total_revenue
        FROM products AS p
        JOIN order_items AS oi ON oi.product_id = p.product_id
        GROUP BY p.product_id, p.name
        ORDER BY total_revenue DESC;
        """
    )
    print_rows("Total revenue per product", rows)


if __name__ == "__main__":
    main()
