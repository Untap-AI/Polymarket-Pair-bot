#!/usr/bin/env python3
"""CSV export for Polymarket pair measurement data.

Usage:
    python scripts/export_data.py attempts --output attempts.csv
    python scripts/export_data.py markets --output markets.csv
    python scripts/export_data.py snapshots --output snapshots.csv
    python scripts/export_data.py summary --output summary.csv
    python scripts/export_data.py lifecycle --output lifecycle.csv
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys

import aiosqlite

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END), 4) as pair_rate,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END), 2) as avg_ttp,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END), 2) as avg_profit,
            ROUND(AVG(CASE WHEN a.status='completed_paired' THEN a.pair_cost_points END), 2) as avg_cost
        FROM ParameterSets p
        LEFT JOIN Attempts a ON p.parameter_set_id = a.parameter_set_id
        GROUP BY p.parameter_set_id
    """,
}


async def export_table(db_path: str, table_key: str, output_path: str) -> None:
    """Query a table and write results to CSV."""
    sql = TABLES.get(table_key)
    if sql is None:
        print(f"Unknown table: {table_key}")
        print(f"Available tables: {', '.join(TABLES.keys())}")
        sys.exit(1)

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        try:
            async with db.execute(sql) as cur:
                rows = await cur.fetchall()
                if not rows:
                    print(f"No data in '{table_key}'.")
                    return

                headers = rows[0].keys()
                with open(output_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(headers)
                    for row in rows:
                        writer.writerow(tuple(row))

                print(f"Exported {len(rows)} rows from '{table_key}' -> {output_path}")
        except Exception as e:
            print(f"Error exporting '{table_key}': {e}")
            sys.exit(1)


def main():
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
    args = parser.parse_args()

    if not os.path.exists(args.db):
        print(f"Database not found: {args.db}")
        sys.exit(1)

    output = args.output or f"{args.table}.csv"
    asyncio.run(export_table(args.db, args.table, output))


if __name__ == "__main__":
    main()
