#!/usr/bin/env python3
"""Build idx_attempts_dashboard on the partitioned Attempts table.

CREATE INDEX CONCURRENTLY is not allowed on the partitioned parent, so we:
  1. Create the parent index shell using CREATE INDEX ON ONLY Attempts
     (no data, instant).
  2. For each child partition, create its own CONCURRENTLY index and then
     ATTACH it to the parent index.  Each partition build is a separate
     statement so Supabase's statement_timeout doesn't apply across all.

Usage:
    python scripts/create_dashboard_index.py [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import load_env_file  # noqa: E402

INDEX_COLS = "(delta_points, S0_points, stop_loss_threshold_points, crypto_asset, t1_timestamp, P1_points, time_remaining_at_start)"
PARENT_INDEX = "idx_attempts_dashboard"


async def run(dry_run: bool) -> None:
    import asyncpg

    load_env_file()
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL not set", file=sys.stderr)
        sys.exit(1)
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]

    conn = await asyncpg.connect(db_url, statement_cache_size=0)

    try:
        # ── 1. Get all child partitions ────────────────────────────────
        partitions = await conn.fetch("""
            SELECT c.relname AS partition_name
            FROM   pg_inherits i
            JOIN   pg_class   c ON c.oid = i.inhrelid
            JOIN   pg_class   p ON p.oid = i.inhparent
            WHERE  p.relname = 'attempts'
            ORDER  BY c.relname
        """)
        print(f"Found {len(partitions)} partitions.\n")

        # ── 2. Check which partitions already have the index ───────────
        existing = set()
        for row in partitions:
            pname = row["partition_name"]
            iname = f"{PARENT_INDEX}_{pname}"
            exists = await conn.fetchval(
                "SELECT 1 FROM pg_indexes WHERE indexname = $1", iname
            )
            if exists:
                existing.add(pname)

        if existing:
            print(f"Already indexed ({len(existing)} partitions): {sorted(existing)}\n")

        todo = [r["partition_name"] for r in partitions if r["partition_name"] not in existing]
        print(f"Partitions to index: {len(todo)}\n")

        if dry_run:
            for p in todo:
                print(f"  would index: {p}")
            return

        # ── 3. Create parent index shell (ON ONLY = no data, instant) ──
        parent_exists = await conn.fetchval(
            "SELECT 1 FROM pg_indexes WHERE indexname = $1", PARENT_INDEX
        )
        if not parent_exists:
            sql = f"CREATE INDEX {PARENT_INDEX} ON ONLY Attempts {INDEX_COLS}"
            print(f"Creating parent index shell: {sql}")
            await conn.execute(sql)
            print("Parent index shell created.\n")
        else:
            print("Parent index shell already exists.\n")

        # ── 4. Build + attach each partition index ─────────────────────
        for i, pname in enumerate(todo, 1):
            iname = f"{PARENT_INDEX}_{pname}"
            print(f"[{i}/{len(todo)}] {pname} → {iname}")

            # Fresh connection per partition so we can SET statement_timeout = 0
            pconn = await asyncpg.connect(db_url, statement_cache_size=0)
            try:
                await pconn.execute("SET statement_timeout = 0")
                build_sql = (
                    f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {iname} "
                    f"ON {pname} {INDEX_COLS}"
                )
                await pconn.execute(build_sql)
                print(f"  built {iname}")
            finally:
                await pconn.close()

            # Attach to parent (must be non-concurrent, but it's instant)
            attach_sql = (
                f"ALTER INDEX {PARENT_INDEX} ATTACH PARTITION {iname}"
            )
            await conn.execute(attach_sql)
            print(f"  attached to {PARENT_INDEX}\n")

        print("All partitions indexed and attached.")

        # ── 5. Verify ──────────────────────────────────────────────────
        invalid = await conn.fetchval(
            "SELECT COUNT(*) FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "WHERE c.relname LIKE $1 AND NOT i.indisvalid",
            f"{PARENT_INDEX}%"
        )
        if invalid:
            print(f"WARNING: {invalid} partition indexes are not yet valid.")
        else:
            print("All partition indexes valid. Index is ready.")

    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    asyncio.run(run(args.dry_run))


if __name__ == "__main__":
    main()
