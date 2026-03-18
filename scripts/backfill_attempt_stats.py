#!/usr/bin/env python3
"""Backfill attempt_stats from existing Attempts data.

Processes one calendar day at a time (exploits ts partition pruning).
Idempotent: deletes then re-inserts each day, so it is safe to re-run.

Usage:
    python scripts/backfill_attempt_stats.py                    # last 30 days
    python scripts/backfill_attempt_stats.py --days 7           # last 7 days
    python scripts/backfill_attempt_stats.py --all              # all history
    python scripts/backfill_attempt_stats.py --date 2026-03-10  # single day
    python scripts/backfill_attempt_stats.py --after 2026-02-01 # from date onwards

Acceptance: after running, verify:
    SELECT SUM(attempts) FROM attempt_stats;
    SELECT COUNT(*) FROM Attempts WHERE status IN ('completed_paired', 'completed_failed') AND S0_points = 1;
    -- These should match.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime, timedelta
from typing import Optional

# Supabase terminates queries that run longer than ~2 minutes at the compute layer.
# Chunk each day into CHUNK_HOURS windows so each INSERT SELECT stays well under that.
_CHUNK_HOURS = 4

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import load_env_file  # noqa: E402

# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

_TAKER_FEE_SQL = (
    "(100.0 * 0.25 * POWER("
    "  ((P1_points - stop_loss_threshold_points) / 100.0)"
    "  * (1.0 - (P1_points - stop_loss_threshold_points) / 100.0),"
    "  2"
    "))"
)

# $1 = window start (timestamp), $2 = window end (timestamp), $3 = attempt_date (date)
# Range predicate exploits partition pruning; $3 locks the attempt_date column.
_INSERT_FOR_WINDOW = f"""
INSERT INTO attempt_stats (
    delta_points, stop_loss_threshold_points, P1_points, time_minute,
    crypto_asset, attempt_date, status, fail_reason,
    first_leg_side, hour_of_day,
    attempts, pairs, total_pnl, sum_time_to_pair, sum_pair_profit
)
SELECT
    delta_points,
    stop_loss_threshold_points,
    P1_points,
    CEIL(time_remaining_at_start / 60.0)::int                   AS time_minute,
    crypto_asset,
    $3::date                                                    AS attempt_date,
    status,
    fail_reason,
    first_leg_side,
    EXTRACT(HOUR FROM t1_timestamp::timestamp)::int             AS hour_of_day,
    COUNT(*)                                                    AS attempts,
    SUM(CASE WHEN status = 'completed_paired' THEN 1 ELSE 0 END) AS pairs,
    SUM(
        CASE
            WHEN status = 'completed_paired' THEN delta_points
            WHEN status = 'completed_failed'
                 AND stop_loss_threshold_points IS NOT NULL
                 AND P1_points >= stop_loss_threshold_points
                THEN -(stop_loss_threshold_points + {_TAKER_FEE_SQL})
            WHEN status = 'completed_failed' THEN -P1_points
            ELSE 0
        END
    )::float                                                    AS total_pnl,
    SUM(COALESCE(time_to_pair_seconds, 0))                      AS sum_time_to_pair,
    SUM(COALESCE(pair_profit_points, 0))                        AS sum_pair_profit
FROM Attempts
WHERE ts >= $1 AND ts < $2
  AND status IN ('completed_paired', 'completed_failed')
  AND S0_points = 1
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
ON CONFLICT (
    delta_points,
    COALESCE(stop_loss_threshold_points, -1),
    P1_points,
    time_minute,
    crypto_asset,
    attempt_date,
    status,
    COALESCE(fail_reason, ''),
    first_leg_side,
    hour_of_day
)
DO UPDATE SET
    attempts      = attempt_stats.attempts      + EXCLUDED.attempts,
    pairs         = attempt_stats.pairs         + EXCLUDED.pairs,
    total_pnl     = attempt_stats.total_pnl     + EXCLUDED.total_pnl,
    sum_time_to_pair = attempt_stats.sum_time_to_pair + EXCLUDED.sum_time_to_pair,
    sum_pair_profit  = attempt_stats.sum_pair_profit  + EXCLUDED.sum_pair_profit
"""


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------

async def backfill_date(db_url: str, d: date) -> int:
    """Delete and re-insert attempt_stats rows for one calendar day.

    Uses a fresh connection per day and splits the day into CHUNK_HOURS windows
    so each INSERT SELECT stays well under Supabase's ~2-min compute limit.
    Returns the number of rows inserted.
    """
    import asyncpg as _asyncpg

    conn = await _asyncpg.connect(db_url, statement_cache_size=0)
    try:
        # Clear the day first (idempotent) — attempt_stats rows are small, this is fast
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '30s'", timeout=10)
            await conn.execute(
                "DELETE FROM attempt_stats WHERE attempt_date = $1", d, timeout=60
            )

        total = 0
        day_start = datetime(d.year, d.month, d.day)
        for h in range(0, 24, _CHUNK_HOURS):
            win_start = day_start + timedelta(hours=h)
            win_end = day_start + timedelta(hours=h + _CHUNK_HOURS)
            async with conn.transaction():
                await conn.execute("SET LOCAL statement_timeout = '5min'", timeout=10)
                # asyncpg timeout=300 ensures we never hang forever on a dead TCP connection
                result = await conn.execute(
                    _INSERT_FOR_WINDOW, win_start, win_end, d, timeout=300
                )
            try:
                chunk_n = int(result.split()[-1])
                total += chunk_n
                print(f"    chunk {h:02d}-{h+_CHUNK_HOURS:02d}h: {chunk_n:,} rows", flush=True)
            except (ValueError, IndexError):
                pass
        return total
    finally:
        await conn.close()


async def _get_date_range(conn) -> tuple[date, date]:
    """Return (min_date, max_date) of completed attempts in the DB."""
    # Partition-friendly: use index scan on partitioned table
    # (MIN/MAX on ts_timestamp is a full scan, but with parallel workers it's fast)
    async with conn.transaction():
        await conn.execute("SET LOCAL statement_timeout = '5min'")
        row = await conn.fetchrow(
            """
            SELECT
                DATE(MIN(t1_timestamp::timestamp)) AS min_date,
                DATE(MAX(t1_timestamp::timestamp)) AS max_date
            FROM Attempts
            WHERE status IN ('completed_paired', 'completed_failed')
              AND S0_points = 1
            """
        )
    if row is None or row["min_date"] is None:
        return date.today(), date.today()
    return row["min_date"], row["max_date"]


async def run(
    db_url: str,
    days: Optional[int] = 30,
    all_history: bool = False,
    single_date: Optional[str] = None,
    after: Optional[str] = None,
) -> None:
    import asyncpg

    if single_date:
        dates = [date.fromisoformat(single_date)]
    else:
        if all_history:
            # Use a short-lived connection for the range query
            conn = await asyncpg.connect(db_url, statement_cache_size=0)
            try:
                min_d, max_d = await _get_date_range(conn)
            finally:
                await conn.close()
            print(f"  Full history: {min_d} → {max_d}")
        else:
            max_d = date.today()
            if after:
                min_d = date.fromisoformat(after)
            else:
                min_d = max_d - timedelta(days=days - 1)  # type: ignore[operator]
            print(f"  Date range: {min_d} → {max_d}")

        dates = []
        d = min_d
        while d <= max_d:
            dates.append(d)
            d += timedelta(days=1)

    print(f"  Processing {len(dates)} date(s) in {_CHUNK_HOURS}-hour chunks…")
    total_rows = 0
    failed_dates = []
    for i, d in enumerate(dates, 1):
        try:
            n = await backfill_date(db_url, d)
            total_rows += n
            print(f"  [{i:3d}/{len(dates)}] {d}  → {n:6,} rows", flush=True)
        except Exception as exc:
            print(f"  [{i:3d}/{len(dates)}] {d}  ERROR: {exc}", flush=True)
            failed_dates.append(d)

    if failed_dates:
        print(f"\n  {len(failed_dates)} date(s) failed: {failed_dates}")
        print("  Re-run with --after <first-failed-date> to retry.")

    print(f"\n  Done.  Total rows inserted this run: {total_rows:,}")

    # Final verification using a fresh connection
    conn = await asyncpg.connect(db_url, statement_cache_size=0)
    try:
        stats_total = await conn.fetchval("SELECT SUM(attempts) FROM attempt_stats")
        print(f"  Verification: attempt_stats.SUM(attempts) = {stats_total:,}")
    finally:
        await conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    load_env_file()
    parser = argparse.ArgumentParser(
        description="Backfill attempt_stats from Attempts table"
    )
    parser.add_argument("--db-url", default=None)
    parser.add_argument("--days", type=int, default=30,
                        help="Number of recent days to backfill (default: 30)")
    parser.add_argument("--all", dest="all_history", action="store_true",
                        help="Backfill all historical data (overrides --days)")
    parser.add_argument("--date", default=None,
                        help="Backfill a single date (YYYY-MM-DD)")
    parser.add_argument("--after", default=None,
                        help="Backfill from this date onwards (YYYY-MM-DD)")
    args = parser.parse_args()

    # Prefer the session pooler (direct port 5432) — supports long-running queries and SET persistence
    db_url = args.db_url or os.environ.get("DATABASE_URL_SESSION") or os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: No DATABASE_URL. Set env var or pass --db-url.", file=sys.stderr)
        sys.exit(1)

    print("\nBackfilling attempt_stats…")
    asyncio.run(run(
        db_url=db_url,
        days=args.days,
        all_history=args.all_history,
        single_date=args.date,
        after=args.after,
    ))


if __name__ == "__main__":
    main()
