#!/usr/bin/env python3
"""Backfill Attempts.crypto_asset from Markets in batches.

Run after migration 015_denormalize_crypto_asset.sql has been applied.

Usage:
    python scripts/backfill_crypto_asset.py [--batch-size N] [--dry-run]

The script pages through Attempts by attempt_id in ascending order,
updating each batch in its own transaction so no single transaction
holds locks on millions of rows. Progress is printed after each batch.

After all rows are backfilled, the index on crypto_asset is created
using CREATE INDEX CONCURRENTLY so it doesn't block active queries.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402


async def run(batch_size: int, dry_run: bool) -> None:
    import asyncpg

    load_env_file()
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)

    # asyncpg expects postgresql:// not postgres://
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]

    conn = await asyncpg.connect(db_url, statement_cache_size=0)

    try:
        await conn.execute("SET statement_timeout = '0'")

        if dry_run:
            total_rows = await conn.fetchval(
                "SELECT COUNT(*) FROM Attempts WHERE crypto_asset IS NULL"
            )
            print(f"Rows to backfill: {total_rows:,}")
            print("Dry run — exiting without changes.")
            return

        # Use MIN/MAX on the PK (index scan, fast) instead of COUNT(*) on 45M rows
        min_id = await conn.fetchval(
            "SELECT MIN(attempt_id) FROM Attempts WHERE crypto_asset IS NULL"
        )
        if min_id is None:
            print("Nothing to backfill.")
        else:
            max_id = await conn.fetchval(
                "SELECT MAX(attempt_id) FROM Attempts WHERE crypto_asset IS NULL"
            )
            print(f"attempt_id range: {min_id:,} – {max_id:,}")
            print(f"Batch size: {batch_size:,}\n")

            updated_total = 0
            cursor = min_id
            t0 = time.monotonic()

            while cursor <= max_id:
                batch_end = cursor + batch_size - 1
                updated = await conn.execute(
                    """
                    UPDATE Attempts a
                    SET    crypto_asset = m.crypto_asset
                    FROM   Markets m
                    WHERE  a.market_id = m.market_id
                      AND  a.crypto_asset IS NULL
                      AND  a.attempt_id BETWEEN $1 AND $2
                    """,
                    cursor,
                    batch_end,
                )
                # asyncpg returns "UPDATE N" as a string
                n = int(updated.split()[-1])
                updated_total += n
                elapsed = time.monotonic() - t0
                rate = updated_total / elapsed if elapsed > 0 else 0
                print(
                    f"  ids {cursor:>10,}–{batch_end:>10,} | "
                    f"updated {n:>6,} | "
                    f"total {updated_total:>8,} | "
                    f"{rate:,.0f} rows/s"
                )
                cursor = batch_end + 1

            print(f"\nBackfill complete: {updated_total:,} rows updated in {time.monotonic()-t0:.1f}s")

        # Create the index concurrently (doesn't block queries)
        print("\nCreating idx_attempts_crypto_asset CONCURRENTLY ...")
        print("(This may take several minutes on 30M rows — safe to leave running)")
        # CONCURRENTLY cannot run inside a transaction block; asyncpg handles this
        # at session level by default.
        await conn.execute(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_attempts_crypto_asset "
            "ON Attempts (crypto_asset)"
        )
        print("Index created.")

    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
        help="Rows per UPDATE batch (default: 50,000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Count rows to update without making changes",
    )
    args = parser.parse_args()
    asyncio.run(run(args.batch_size, args.dry_run))


if __name__ == "__main__":
    main()
