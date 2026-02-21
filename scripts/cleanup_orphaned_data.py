#!/usr/bin/env python3
"""Delete attempts (and their lifecycle rows) that are missing stop_loss_threshold_points,
then prune any markets no longer referenced by any attempt or snapshot.

Usage:
    python scripts/cleanup_orphaned_data.py              # dry-run (default)
    python scripts/cleanup_orphaned_data.py --execute    # actually delete
    python scripts/cleanup_orphaned_data.py --db-url <url>
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402


def _resolve_db_url(args) -> str:
    url = (
        getattr(args, "db_url", None)
        or os.environ.get("DATABASE_URL_SESSION")
        or os.environ.get("DATABASE_URL")
    )
    if not url:
        print("Error: No database URL provided.")
        print("  Use --db-url or set DATABASE_URL_SESSION or DATABASE_URL.")
        sys.exit(1)
    if "postgres" not in url.lower():
        print("Error: This script only supports PostgreSQL. Got:", url[:30])
        sys.exit(1)
    return url


PREVIEW_SQL = """
SELECT
    (SELECT COUNT(*) FROM attemptlifecycle
     WHERE attempt_id IN (
         SELECT attempt_id FROM Attempts WHERE stop_loss_threshold_points IS NULL
     )) AS lifecycle_rows_to_delete,

    (SELECT COUNT(*) FROM Attempts
     WHERE stop_loss_threshold_points IS NULL) AS attempts_to_delete,

    (SELECT COUNT(*) FROM Markets
     WHERE market_id NOT IN (SELECT DISTINCT market_id FROM Attempts)
       AND market_id NOT IN (SELECT DISTINCT market_id FROM Snapshots)
    ) AS markets_to_delete;
"""

CLEANUP_SQL = """
BEGIN;

DELETE FROM attemptlifecycle
WHERE attempt_id IN (
    SELECT attempt_id FROM Attempts
    WHERE stop_loss_threshold_points IS NULL
);

DELETE FROM Attempts
WHERE stop_loss_threshold_points IS NULL;

DELETE FROM Markets
WHERE market_id NOT IN (SELECT DISTINCT market_id FROM Attempts)
  AND market_id NOT IN (SELECT DISTINCT market_id FROM Snapshots);

COMMIT;
"""


async def run(args) -> None:
    import asyncpg

    url = _resolve_db_url(args)
    dry_run = not args.execute

    pool = await asyncpg.create_pool(url, min_size=1, max_size=3, statement_cache_size=0)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(PREVIEW_SQL)
            lifecycle_count = row["lifecycle_rows_to_delete"]
            attempts_count  = row["attempts_to_delete"]
            markets_count   = row["markets_to_delete"]

            print("Rows that would be deleted:")
            print(f"  attemptlifecycle rows : {lifecycle_count}")
            print(f"  Attempts rows         : {attempts_count}")
            print(f"  Markets rows          : {markets_count}")

            if dry_run:
                print("\nDry-run mode — nothing deleted. Pass --execute to apply.")
                return

            if lifecycle_count == 0 and attempts_count == 0 and markets_count == 0:
                print("\nNothing to delete — database is already clean.")
                return

            confirm = input("\nProceed with deletion? [y/N] ").strip().lower()
            if confirm != "y":
                print("Aborted.")
                return

            await conn.execute(CLEANUP_SQL)
            print("Done. Rows deleted successfully.")
    finally:
        await pool.close()


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(description="Clean up orphaned attempts and markets")
    parser.add_argument("--db-url", default=None, help="PostgreSQL connection URL")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually run the deletes (default is dry-run)",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
