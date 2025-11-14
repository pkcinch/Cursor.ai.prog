#!/usr/bin/env python3
"""Execute reporting queries against the e-commerce SQLite database."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT_DIR / "database" / "ecom.db"


def parse_args() -> argparse.Namespace:
    """Parse CLI options for database selection."""
    parser = argparse.ArgumentParser(description="Run predefined analytics queries")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    return parser.parse_args()


def fetch(cursor: sqlite3.Cursor, sql: str) -> Tuple[Sequence[str], Iterable[sqlite3.Row]]:
    """Execute SQL and return column headers plus rows."""
    cursor.execute(sql)
    headers = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return headers, rows


def print_table(title: str, headers: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    """Print data in a simple tabular layout."""
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


def run_queries(db_path: Path) -> None:
    """Open the database and run each aggregate JOIN query."""
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Query 1: Total revenue per product using joins between order_items,
        # orders, and the product catalog.
        product_revenue_sql = """
            SELECT
                p.product_id,
                p.name AS product_name,
                p.category,
                printf('%.2f', SUM(oi.quantity * oi.price)) AS total_revenue,
                COUNT(DISTINCT oi.order_id) AS orders_involved
            FROM products AS p
            JOIN order_items AS oi ON oi.product_id = p.product_id
            JOIN orders AS o ON o.order_id = oi.order_id
            GROUP BY p.product_id, p.name, p.category
            ORDER BY CAST(total_revenue AS REAL) DESC;
        """
        headers, rows = fetch(cur, product_revenue_sql)
        print_table("Total revenue per product", headers, rows)

        # Query 2: Top 10 customers by total spending via users + orders + items.
        top_customers_sql = """
            SELECT
                u.user_id,
                u.name,
                u.email,
                printf('%.2f', SUM(oi.quantity * oi.price)) AS total_spent,
                COUNT(DISTINCT o.order_id) AS orders_placed
            FROM users AS u
            JOIN orders AS o ON o.user_id = u.user_id
            JOIN order_items AS oi ON oi.order_id = o.order_id
            GROUP BY u.user_id, u.name, u.email
            ORDER BY CAST(total_spent AS REAL) DESC
            LIMIT 10;
        """
        headers, rows = fetch(cur, top_customers_sql)
        print_table("Top 10 customers by spending", headers, rows)

        # Query 3: Average rating per product joining reviews with products.
        avg_rating_sql = """
            SELECT
                p.product_id,
                p.name AS product_name,
                printf('%.2f', AVG(r.rating)) AS avg_rating,
                COUNT(r.review_id) AS review_count
            FROM products AS p
            JOIN reviews AS r ON r.product_id = p.product_id
            GROUP BY p.product_id, p.name
            HAVING review_count > 0
            ORDER BY CAST(avg_rating AS REAL) DESC, review_count DESC;
        """
        headers, rows = fetch(cur, avg_rating_sql)
        print_table("Average rating per product", headers, rows)


def main() -> None:
    args = parse_args()
    run_queries(args.db)


if __name__ == "__main__":
    main()
