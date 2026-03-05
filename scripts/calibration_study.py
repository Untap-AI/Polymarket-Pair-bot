#!/usr/bin/env python3
"""Calibration study: are Polymarket crypto binary markets efficiently priced?

For each (minute_remaining, price_bucket) we compute:
  - sample count
  - settlement rate (fraction that settled YES)
  - calibration error = settlement_rate - implied_probability (positive = market underprices YES)

Uses P1 price at time remaining from Attempts (first-leg entry). YES-side price = P1 if first_leg_side=YES else 100-P1.
One observation per attempt in the final 15 minutes. Does not require Snapshots.

Usage:
  python scripts/calibration_study.py
  python scripts/calibration_study.py --bucket-size 2 --output calibration.csv
  python scripts/calibration_study.py --market-id btc-updown-15m-1768502700   # single market view
  python scripts/calibration_study.py --market-ids markets.txt                # filter by list
  python scripts/calibration_study.py --heatmap
  python scripts/calibration_study.py --use-snapshots   # use Snapshots table instead of Attempts (if available)

Regions of interest for pair trading: 50-68¢ at 2-4 min, 80-87¢ at 5-7 min.
Positive calibration error in those cells suggests the market underprices YES (structural edge).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import math
import os
import subprocess
import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402
from src.metrics import _is_pg  # noqa: E402


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MINUTES_FINAL = 15  # 1..15 minutes remaining
SECONDS_PER_MINUTE = 60
STATEMENT_TIMEOUT_MS = 600_000  # 10 minutes — Attempts table is large


@asynccontextmanager
async def _connect_with_timeout(db_source: str):
    """Connect with a generous statement timeout for heavy analytics queries."""
    if _is_pg(db_source):
        import asyncpg
        conn = await asyncpg.connect(
            db_source,
            statement_cache_size=0,
            server_settings={"statement_timeout": str(STATEMENT_TIMEOUT_MS)},
        )
        try:
            yield _PgAdapter(conn)
        finally:
            await conn.close()
    else:
        import aiosqlite
        db = await aiosqlite.connect(db_source)
        db.row_factory = aiosqlite.Row
        try:
            yield _SqliteAdapter(db)
        finally:
            await db.close()


class _PgAdapter:
    def __init__(self, conn):
        self._conn = conn

    async def fetch_all(self, sql, params=None):
        from src.metrics import _q
        params = params or []
        rows = await self._conn.fetch(_q(sql), *params)
        return [dict(r) for r in rows]


class _SqliteAdapter:
    def __init__(self, db):
        self._db = db

    async def fetch_all(self, sql, params=None):
        params = params or []
        async with self._db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


def _db_source(args) -> str:
    """Prefer session pooler (port 5432) for long-running analytics queries."""
    return (
        getattr(args, "db_url", None)
        or os.environ.get("DATABASE_URL_SESSION")
        or os.environ.get("DATABASE_URL")
        or ""
    )


async def _fetch_markets_with_outcomes(
    conn, db_source: str, market_ids: list[str] | None, crypto_asset: str | None
) -> list[dict]:
    """Return markets that have winning_outcome in ('yes','no')."""
    clauses = ["winning_outcome IN ('yes','no')"]
    params: list = []
    if market_ids is not None:
        if _is_pg(db_source):
            clauses.append("market_id = ANY($1)")
            params.append(market_ids)
        else:
            placeholders = ",".join("?" * len(market_ids))
            clauses.append(f"market_id IN ({placeholders})")
            params.extend(market_ids)
    if crypto_asset is not None:
        if _is_pg(db_source):
            params.append(crypto_asset.lower())
            clauses.append(f"crypto_asset = ${len(params)}")
        else:
            params.append(crypto_asset.lower())
            clauses.append("crypto_asset = ?")
    sql = "SELECT market_id, winning_outcome, crypto_asset FROM Markets WHERE " + " AND ".join(clauses)
    return await conn.fetch_all(sql, params)


async def _fetch_calibration_agg(conn, db_source: str) -> list[dict]:
    """Aggregate calibration data entirely in SQL.

    For each (yes_price, minute_remaining) bucket, count total attempts
    and how many settled YES. Uses the partial index on Attempts and
    JOINs to Markets for winning_outcome.

    Returns small result set (~1500 rows max) so no timeout risk.
    """
    sql = """
        SELECT
            CASE WHEN a.first_leg_side = 'YES' THEN a.P1_points
                 ELSE 100 - a.P1_points END AS yes_price,
            CEIL(a.time_remaining_at_start / 60)::int AS minute_remaining,
            COUNT(*) AS total,
            SUM(CASE WHEN m.winning_outcome = 'yes' THEN 1 ELSE 0 END) AS settled_yes
        FROM Attempts a
        INNER JOIN Markets m ON a.market_id = m.market_id
        WHERE a.status IN ('completed_paired', 'completed_failed')
          AND a.S0_points = 1
          AND a.time_remaining_at_start > 0
          AND a.time_remaining_at_start <= ?
          AND m.winning_outcome IN ('yes', 'no')
          AND a.first_leg_side IN ('YES', 'NO')
        GROUP BY yes_price, minute_remaining
        ORDER BY yes_price, minute_remaining
    """
    return await conn.fetch_all(sql, [MINUTES_FINAL * SECONDS_PER_MINUTE])


async def _fetch_snapshots_for_markets(conn, db_source: str, market_ids: list[str]) -> list[dict]:
    """Fetch all snapshots for given market_ids with time_remaining in (0, 15*60]."""
    if not market_ids:
        return []
    try:
        if _is_pg(db_source):
            sql = """SELECT market_id, time_remaining, yes_ask_points, yes_last_trade_points
                     FROM Snapshots
                     WHERE market_id = ANY($1) AND time_remaining > 0 AND time_remaining <= $2
                     ORDER BY market_id, time_remaining"""
            return await conn.fetch_all(sql, [market_ids, MINUTES_FINAL * SECONDS_PER_MINUTE])
        placeholders = ",".join("?" * len(market_ids))
        sql = f"""SELECT market_id, time_remaining, yes_ask_points, yes_last_trade_points
                  FROM Snapshots
                  WHERE market_id IN ({placeholders}) AND time_remaining > 0 AND time_remaining <= ?
                  ORDER BY market_id, time_remaining"""
        return await conn.fetch_all(sql, market_ids + [MINUTES_FINAL * SECONDS_PER_MINUTE])
    except Exception as e:
        err_msg = str(e).lower()
        if "snapshots" in err_msg and ("does not exist" in err_msg or "undefined" in err_msg):
            print(
                "Warning: Snapshots table not found.",
                file=sys.stderr,
            )
            print(
                "  Create it with: python scripts/migrate.py apply",
                file=sys.stderr,
            )
            print(
                "  Snapshot data is collected only when the bot runs with enable_snapshots (cannot be backfilled).",
                file=sys.stderr,
            )
            return []
        raise


def _bucket_price(price_points: int, bucket_size_points: int) -> int:
    """Bucket price (1-99¢). E.g. bucket_size 1 -> same; 2 -> 2,4,...,98; 5 -> 5,10,...,95."""
    if bucket_size_points <= 0:
        bucket_size_points = 1
    # Clamp to 1-99
    p = max(1, min(99, price_points))
    return (p // bucket_size_points) * bucket_size_points or bucket_size_points


def _bucket_center(bucket: int, bucket_size_points: int) -> float:
    """Center of bucket in cents for implied probability."""
    return bucket + bucket_size_points / 2.0


def _run_outcome_backfill(db_url: str) -> None:
    """Run outcome backfill so markets have winning_outcome set."""
    script_dir = Path(__file__).resolve().parent
    cmd = [sys.executable, str(script_dir / "backfill_outcomes.py"), "--execute"]
    env = os.environ.copy()
    if db_url:
        env["DATABASE_URL_SESSION"] = db_url
        env["DATABASE_URL"] = db_url
    print("Running outcome backfill...")
    r = subprocess.run(cmd, env=env, cwd=script_dir.parent)
    if r.returncode != 0:
        print("Warning: backfill exited with code", r.returncode, file=sys.stderr)
    else:
        print("Outcome backfill done.")


async def run(args) -> None:
    load_env_file()
    db_source = _db_source(args)
    if not db_source:
        print("Error: set DATABASE_URL or DATABASE_URL_SESSION or pass --db-url")
        sys.exit(1)

    if getattr(args, "run_backfill", False):
        _run_outcome_backfill(db_source)

    bucket_size = max(1, int(getattr(args, "bucket_size", 1)))
    price_source = getattr(args, "price_source", "yes_ask") or "yes_ask"
    crypto_asset = getattr(args, "crypto_asset", None)
    if crypto_asset:
        crypto_asset = crypto_asset.strip().lower()
    market_ids_filter: list[str] | None = None
    if getattr(args, "market_id", None):
        market_ids_filter = [args.market_id.strip()]
    elif getattr(args, "market_ids", None):
        path = Path(args.market_ids)
        if path.exists():
            market_ids_filter = [line.strip() for line in path.read_text().splitlines() if line.strip()]
        else:
            print(f"Error: file not found: {path}")
            sys.exit(1)

    async with _connect_with_timeout(db_source) as conn:
        print("  Running calibration aggregation query...")
        agg_rows = await _fetch_calibration_agg(conn, db_source)
        print(f"  Got {len(agg_rows)} (price, minute) buckets from DB.")

    if not agg_rows:
        print("No attempt data found. Make sure the bot has run and has completed attempts.")
        sys.exit(0)

    # Build price summary (across all minutes) and per-cell data from SQL results
    price_counts: dict[int, tuple[int, int]] = defaultdict(lambda: (0, 0))
    cell_counts: dict[tuple[int, int], tuple[int, int]] = defaultdict(lambda: (0, 0))
    total_observations = 0

    for row in agg_rows:
        yes_price = int(row["yes_price"])
        minute = int(row["minute_remaining"])
        total = int(row["total"])
        settled_yes = int(row["settled_yes"])
        if not 1 <= yes_price <= 99 or not 1 <= minute <= MINUTES_FINAL:
            continue
        total_observations += total
        # Per-price summary
        tp, yp = price_counts[yes_price]
        price_counts[yes_price] = (tp + total, yp + settled_yes)
        # Per (minute, bucket) cell
        bucket = _bucket_price(yes_price, bucket_size)
        tc, yc = cell_counts[(minute, bucket)]
        cell_counts[(minute, bucket)] = (tc + total, yc + settled_yes)

    rows = []
    for (minute, bucket), (total, yes_count) in sorted(cell_counts.items()):
        rate = yes_count / total if total else 0.0
        center = _bucket_center(bucket, bucket_size)
        implied = center / 100.0
        cal_error = rate - implied
        rows.append({
            "minute_remaining": minute,
            "price_bucket": bucket,
            "price_center_cents": round(center, 2),
            "total": total,
            "settled_yes": yes_count,
            "settlement_rate": round(rate, 4),
            "calibration_error": round(cal_error, 4),
        })

    # Console output
    print()
    print("=" * 72)
    print("  CALIBRATION STUDY — YES price vs actual settlement rate")
    print("=" * 72)
    print(f"  Observations: {total_observations:,}")
    if crypto_asset:
        print(f"  Crypto asset: {crypto_asset}")
    print()
    print(f"  {'price':>5}  {'total':>10}  {'settled_yes':>11}  {'rate':>7}  {'implied':>7}  {'error':>7}")
    print(f"  {'-----':>5}  {'----------':>10}  {'-----------':>11}  {'-------':>7}  {'-------':>7}  {'-------':>7}")
    for price in range(1, 100):
        total_p, yes_p = price_counts.get(price, (0, 0))
        if total_p == 0:
            continue
        rate_p = yes_p / total_p
        implied = price / 100.0
        error = rate_p - implied
        sign = "+" if error >= 0 else ""
        print(f"  {price:>4}c  {total_p:>10,}  {yes_p:>11,}  {rate_p:>7.2%}  {implied:>7.2%}  {sign}{error:>6.2%}")
    print()

    # CSV output (optional)
    out_path = getattr(args, "output", None)
    if out_path:
        with open(out_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["minute_remaining", "price_bucket", "price_center_cents", "total", "settled_yes", "settlement_rate", "calibration_error"])
            w.writeheader()
            w.writerows(rows)
        print(f"Wrote {len(rows)} rows to {out_path}")

    if getattr(args, "heatmap", False) and rows:
        _print_heatmap(rows, bucket_size)


def _print_heatmap(rows: list[dict], bucket_size: int) -> None:
    """Simple text heatmap: rows = minutes 1-15, cols = price buckets, value = calibration error."""
    from collections import defaultdict
    grid: dict[tuple[int, int], float] = {(r["minute_remaining"], r["price_bucket"]): r["calibration_error"] for r in rows}
    minutes = sorted({r["minute_remaining"] for r in rows})
    buckets = sorted({r["price_bucket"] for r in rows})
    if not buckets:
        return
    print("\nCalibration error heatmap (rows=minute, cols=price bucket; + = underpricing YES)")
    header = "min\t" + "\t".join(str(b) for b in buckets[:20])
    if len(buckets) > 20:
        header += "\t..."
    print(header)
    for m in minutes:
        line = [str(m)]
        for b in buckets[:20]:
            v = grid.get((m, b))
            if v is None:
                line.append(".")
            else:
                line.append(f"{v:+.2f}")
        if len(buckets) > 20:
            line.append("...")
        print("\t".join(line))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Calibration study: efficiency of Polymarket crypto binary prices in final 15 minutes"
    )
    parser.add_argument("--db-url", default=None, help="PostgreSQL or SQLite URL/path")
    parser.add_argument("--bucket-size", type=int, default=1, metavar="CENTS", help="Price bucket size in cents (1, 2, 5)")
    parser.add_argument("--price-source", choices=["yes_ask", "yes_last_trade"], default="yes_ask", help="YES price from Snapshots when --use-snapshots")
    parser.add_argument("--use-snapshots", action="store_true", help="Use Snapshots table instead of Attempts (requires enable_snapshots)")
    parser.add_argument("--output", "-o", default=None, help="Write CSV to this path")
    parser.add_argument("--heatmap", action="store_true", help="Print text heatmap of calibration error")
    parser.add_argument("--market-id", default=None, help="Restrict to a single market_id (market-by-market view)")
    parser.add_argument("--market-ids", default=None, help="Path to file with one market_id per line (filter to these markets)")
    parser.add_argument("--crypto-asset", default=None, help="Restrict to one asset (e.g. btc, eth, sol, xrp); default all four")
    parser.add_argument("--run-backfill", action="store_true", help="Run outcome backfill (backfill_outcomes.py) before querying")
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
