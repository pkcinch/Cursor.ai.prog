#!/usr/bin/env python3
"""Run standard reports against database/ecom.db."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence, Tuple

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB = BASE_DIR / "database" / "ecom.db"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute canned SQL queries")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    return parser.parse_args()


def fetch(cursor: sqlite3.Cursor, sql: str) -> Tuple[Sequence[str], Iterable[sqlite3.Row]]:
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    return headers, cursor.fetchall()


def print_table(title: str, headers: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    print(f"\n{title}\n" + "-" * len(title))
    if not rows:
        print("(no rows)")
        return
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))
    header_line = " | ".join(f"{header:<{widths[idx]}}" for idx, header in enumerate(headers))
    divider = "-+-".join("-" * w for w in widths)
    print(header_line)
    print(divider)
    for row in rows:
        print(" | ".join(f"{value!s:<{widths[idx]}}" for idx, value in enumerate(row)))


def run_reports(db_path: Path) -> None:
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        all_customers = """
            SELECT user_id, first_name, last_name, email, signup_date, country
            FROM users
            ORDER BY user_id;
        """
        headers, rows = fetch(cur, all_customers)
        print_table("All customers", headers, rows)

        all_products = """
            SELECT product_id, name, category, printf('%.2f', price) AS price, stock
            FROM products
            ORDER BY product_id;
        """
        headers, rows = fetch(cur, all_products)
        print_table("All products", headers, rows)

        all_orders = """
            SELECT order_id, user_id, order_date, status, printf('%.2f', total_amount) AS total_amount
            FROM orders
            ORDER BY order_date;
        """
        headers, rows = fetch(cur, all_orders)
        print_table("All orders", headers, rows)

        total_sales = """
            SELECT printf('%.2f', SUM(line_total)) AS total_sales
            FROM order_items;
        """
        headers, rows = fetch(cur, total_sales)
        print_table("Total sales", headers, rows)

        top_product = """
            SELECT
                p.product_id,
                p.name AS product_name,
                SUM(oi.quantity) AS units_sold,
                printf('%.2f', SUM(oi.line_total)) AS revenue
            FROM products AS p
            JOIN order_items AS oi ON oi.product_id = p.product_id
            GROUP BY p.product_id, p.name
            ORDER BY units_sold DESC, CAST(revenue AS REAL) DESC
            LIMIT 5;
        """
        headers, rows = fetch(cur, top_product)
        print_table("Top selling products", headers, rows)


def main() -> None:
    args = parse_args()
    run_reports(args.db)


if __name__ == "__main__":
    main()
