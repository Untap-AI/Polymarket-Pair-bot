#!/usr/bin/env python3
"""CSV export for Polymarket pair measurement data.

Usage:
    python scripts/export_data.py attempts --output attempts.csv
    python scripts/export_data.py markets --output markets.csv
    python scripts/export_data.py summary --output summary.csv
    python scripts/export_data.py attempts --db-url 'postgres://…'

The database source is resolved in order:
  1. --db-url flag
  2. DATABASE_URL environment variable
  3. --db flag (SQLite file path, default: data/measurements.db)
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402
from src.metrics import _connect  # noqa: E402


TABLES = {
    "attempts": "SELECT * FROM Attempts ORDER BY attempt_id",
    "markets": "SELECT * FROM Markets ORDER BY market_id",
    "snapshots": "SELECT * FROM Snapshots ORDER BY snapshot_id",
    "parameters": "SELECT * FROM ParameterSets ORDER BY parameter_set_id",
    "lifecycle": "SELECT * FROM AttemptLifecycle ORDER BY lifecycle_id",
    "summary": """
        SELECT
            p.name as param_name,
            p.S0_points,
            p.delta_points,
            COUNT(a.attempt_id) as attempts,
            SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END) as pairs,
            SUM(CASE WHEN a.status='completed_failed' THEN 1 ELSE 0 END) as failed,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END)::numeric, 4) as pair_rate,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END)::numeric, 2) as avg_ttp,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END)::numeric, 2) as avg_profit,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.pair_cost_points END)::numeric, 2) as avg_cost
        FROM ParameterSets p
        LEFT JOIN Attempts a ON p.parameter_set_id = a.parameter_set_id
        GROUP BY p.parameter_set_id, p.name, p.S0_points, p.delta_points
    """,
}

# SQLite-compatible summary (no ::numeric cast)
SUMMARY_SQL_SQLITE = """
    SELECT
        p.name as param_name,
        p.S0_points,
        p.delta_points,
        COUNT(a.attempt_id) as attempts,
        SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END) as pairs,
        SUM(CASE WHEN a.status='completed_failed' THEN 1 ELSE 0 END) as failed,
        ROUND(AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END), 4) as pair_rate,
        ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END), 2) as avg_ttp,
        ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END), 2) as avg_profit,
        ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.pair_cost_points END), 2) as avg_cost
    FROM ParameterSets p
    LEFT JOIN Attempts a ON p.parameter_set_id = a.parameter_set_id
    GROUP BY p.parameter_set_id
"""


async def export_table(db_source: str, table_key: str, output_path: str) -> None:
    """Query a table and write results to CSV."""
    sql = TABLES.get(table_key)
    if sql is None:
        print(f"Unknown table: {table_key}")
        print(f"Available tables: {', '.join(TABLES.keys())}")
        sys.exit(1)

    is_pg = "postgres" in db_source.lower()

    # Use SQLite-compatible summary when not on PG
    if table_key == "summary" and not is_pg:
        sql = SUMMARY_SQL_SQLITE

    async with _connect(db_source) as db:
        try:
            rows = await db.fetch_all(sql)
            if not rows:
                print(f"No data in '{table_key}'.")
                return

            headers = list(rows[0].keys())
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                for row in rows:
                    writer.writerow([row[h] for h in headers])

            print(f"Exported {len(rows)} rows from '{table_key}' -> {output_path}")
        except Exception as e:
            print(f"Error exporting '{table_key}': {e}")
            sys.exit(1)


def _resolve_db_source(args) -> str:
    """Return the database source string (URL or file path)."""
    if args.db_url:
        return args.db_url
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
    return args.db


def main():
    # Load .env file if it exists
    load_env_file()
    
    parser = argparse.ArgumentParser(
        description="Export Polymarket pair measurement data to CSV"
    )
    parser.add_argument(
        "table",
        choices=list(TABLES.keys()),
        help=f"Table to export: {', '.join(TABLES.keys())}",
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output CSV file path (default: <table>.csv)",
    )
    parser.add_argument(
        "--db",
        default="data/measurements.db",
        help="Path to SQLite database (default: data/measurements.db)",
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="PostgreSQL connection URL (overrides --db and DATABASE_URL)",
    )
    args = parser.parse_args()

    db_source = _resolve_db_source(args)

    # Only check file existence for SQLite paths
    if "postgres" not in db_source.lower() and not os.path.exists(db_source):
        print(f"Database not found: {db_source}")
        sys.exit(1)

    output = args.output or f"{args.table}.csv"
    asyncio.run(export_table(db_source, args.table, output))


if __name__ == "__main__":
    main()
