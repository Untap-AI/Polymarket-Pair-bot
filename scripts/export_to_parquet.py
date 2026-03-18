#!/usr/bin/env python3
"""Export Attempts table to date-partitioned Parquet files for DuckDB analysis.

Exports one calendar day at a time using partition pruning.
Output layout:
    data/parquet/attempts/date=YYYY-MM-DD/data.parquet

DuckDB can then scan these with:
    SELECT * FROM read_parquet('.../attempts/**/*.parquet', hive_partitioning=true)

Usage:
    python scripts/export_to_parquet.py                  # today
    python scripts/export_to_parquet.py --days 30        # last 30 days
    python scripts/export_to_parquet.py --date 2026-03-10
    python scripts/export_to_parquet.py --after 2026-02-01
    python scripts/export_to_parquet.py --all            # full history
    python scripts/export_to_parquet.py --sync           # only missing dates

--sync compares dates in attempt_stats against local Parquet files and exports
only the gaps.  Run it whenever you want your Parquet cache up to date before
running liquidity_analysis.py --use-parquet or dedup_vs_all.py --use-parquet.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import load_env_file  # noqa: E402

# Columns exported — covers everything needed by the analysis scripts
_SELECT_COLS = """
    attempt_id,
    market_id,
    parameter_set_id,
    t1_timestamp,
    first_leg_side,
    P1_points,
    reference_yes_points,
    status,
    t2_timestamp,
    time_to_pair_seconds,
    time_remaining_at_start,
    time_remaining_at_completion,
    actual_opposite_price,
    pair_cost_points,
    pair_profit_points,
    fail_reason,
    closest_approach_points,
    max_adverse_excursion_points,
    yes_spread_entry_points,
    no_spread_entry_points,
    yes_spread_exit_points,
    no_spread_exit_points,
    delta_points,
    S0_points,
    stop_loss_threshold_points,
    yes_best_bid_size,
    yes_best_ask_size,
    no_best_bid_size,
    no_best_ask_size,
    yes_ask_depth_2tick,
    no_ask_depth_2tick,
    crypto_asset
"""

# Range predicate exploits partition pruning; ts::date = $1 scans all partitions.
_FETCH_SQL = f"""
SELECT {_SELECT_COLS}
FROM Attempts
WHERE ts >= $1::timestamp AND ts < ($1::date + INTERVAL '1 day')::timestamp
  AND status IN ('completed_paired', 'completed_failed')
  AND S0_points = 1
ORDER BY t1_timestamp
"""


def _local_dates(out_dir: Path) -> set[date]:
    """Return the set of dates that already have a Parquet file on disk."""
    result: set[date] = set()
    for p in out_dir.glob("date=*/data.parquet"):
        try:
            result.add(date.fromisoformat(p.parent.name[len("date="):]))
        except ValueError:
            pass
    return result


async def _db_dates(conn) -> list[date]:
    """Return sorted list of distinct attempt_dates in attempt_stats (fast, <1s)."""
    rows = await conn.fetch(
        "SELECT DISTINCT attempt_date FROM attempt_stats ORDER BY 1"
    )
    return [r["attempt_date"] for r in rows]


async def export_date(conn, d: date, out_dir: Path) -> int:
    """Export one calendar day to a Parquet file. Returns row count."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows = await conn.fetch(_FETCH_SQL, d)
    if not rows:
        return 0

    # Convert asyncpg Records to a dict of lists for pyarrow
    cols: dict[str, list] = {}
    for row in rows:
        for k, v in dict(row).items():
            cols.setdefault(k, []).append(v)

    table = pa.table(cols)

    part_dir = out_dir / f"date={d.isoformat()}"
    part_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, part_dir / "data.parquet", compression="snappy")
    return len(rows)


async def run(
    db_url: str,
    days: int = 1,
    all_history: bool = False,
    sync: bool = False,
    single_date: Optional[str] = None,
    after: Optional[str] = None,
    out_root: Optional[str] = None,
) -> None:
    import asyncpg

    out_dir = Path(out_root or "data/parquet/attempts")

    conn = await asyncpg.connect(db_url, statement_cache_size=0)
    try:
        await conn.execute("SET statement_timeout = '30min'")

        if single_date:
            dates = [date.fromisoformat(single_date)]
        elif sync:
            db = await _db_dates(conn)
            local = _local_dates(out_dir)
            dates = [d for d in db if d not in local]
            print(f"Sync: {len(db)} dates in DB, {len(local)} local, {len(dates)} missing.")
        else:
            if all_history:
                row = await conn.fetchrow(
                    "SELECT DATE(MIN(t1_timestamp::timestamp)) AS min_d,"
                    "       DATE(MAX(t1_timestamp::timestamp)) AS max_d"
                    " FROM Attempts WHERE status IN ('completed_paired','completed_failed')"
                    " AND S0_points=1"
                )
                min_d = row["min_d"] if row and row["min_d"] else date.today()
                max_d = row["max_d"] if row and row["max_d"] else date.today()
            else:
                max_d = date.today()
                min_d = date.fromisoformat(after) if after else max_d - timedelta(days=days - 1)

            dates = []
            d = min_d
            while d <= max_d:
                dates.append(d)
                d += timedelta(days=1)

        if not dates:
            print("Nothing to export — Parquet cache is up to date.")
            return

        print(f"Exporting {len(dates)} date(s) to {out_dir}/…")
        total = 0
        for i, d in enumerate(dates, 1):
            n = await export_date(conn, d, out_dir)
            total += n
            print(f"  [{i:3d}/{len(dates)}] {d}  {n:8,} rows")

        print(f"\nDone.  {total:,} rows exported.")
    finally:
        await conn.close()


def main() -> None:
    load_env_file()
    parser = argparse.ArgumentParser(description="Export Attempts to Parquet for DuckDB")
    parser.add_argument("--db-url", default=None)
    parser.add_argument("--days", type=int, default=1,
                        help="Number of recent days to export (default: 1 = today)")
    parser.add_argument("--all", dest="all_history", action="store_true",
                        help="Export full history")
    parser.add_argument("--sync", action="store_true",
                        help="Export only dates present in attempt_stats but missing locally")
    parser.add_argument("--date", default=None, help="Export a single date (YYYY-MM-DD)")
    parser.add_argument("--after", default=None, help="Export from date onwards (YYYY-MM-DD)")
    parser.add_argument("--out", default=None, help="Output root directory (default: data/parquet/attempts)")
    args = parser.parse_args()

    # Prefer session pooler — supports long-running queries
    db_url = args.db_url or os.environ.get("DATABASE_URL_SESSION") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: No DATABASE_URL.", file=sys.stderr)
        sys.exit(1)

    asyncio.run(run(
        db_url=db_url,
        days=args.days,
        all_history=args.all_history,
        sync=args.sync,
        single_date=args.date,
        after=args.after,
        out_root=args.out,
    ))


if __name__ == "__main__":
    main()
