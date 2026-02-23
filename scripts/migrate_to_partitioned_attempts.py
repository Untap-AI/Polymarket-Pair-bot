#!/usr/bin/env python3
"""Run the partitioned Attempts migration end-to-end.

Runs all SQL steps in succession with batched data migration to avoid timeouts.
Use this instead of the Supabase SQL Editor, which has strict timeout limits.

Usage:
    python scripts/migrate_to_partitioned_attempts.py              # run all steps
    python scripts/migrate_to_partitioned_attempts.py --step 4     # start from step 4
    python scripts/migrate_to_partitioned_attempts.py --dry-run     # show what would run
    python scripts/migrate_to_partitioned_attempts.py --skip-drop  # skip step 7 (drop Attempts_old)
    python scripts/migrate_to_partitioned_attempts.py --step 4 --resume  # resume after timeout

Database URL is resolved from DATABASE_URL_SESSION or DATABASE_URL.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402


def _resolve_db_url() -> str:
    url = os.environ.get("DATABASE_URL_SESSION") or os.environ.get("DATABASE_URL")
    if not url:
        print("Error: No database URL. Set DATABASE_URL_SESSION or DATABASE_URL.")
        sys.exit(1)
    if "postgres" not in url.lower():
        print("Error: This script only supports PostgreSQL.")
        sys.exit(1)
    return url


# ---------------------------------------------------------------------------
# Step 1: CREATE INDEX CONCURRENTLY (no transaction)
# ---------------------------------------------------------------------------
STEP1_SQL = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_attempts_optimize_params
    ON Attempts (
        delta_points,
        stop_loss_threshold_points,
        P1_points,
        time_remaining_at_start,
        t1_timestamp
    )
    WHERE status IN ('completed_paired', 'completed_failed')
      AND S0_points = 1
      AND time_remaining_at_start <= 900;
"""

# ---------------------------------------------------------------------------
# Step 2: Drop redundant indexes (in transaction)
# ---------------------------------------------------------------------------
STEP2_SQL = """
DROP INDEX IF EXISTS idx_attempts_status;
DROP INDEX IF EXISTS idx_attempts_delta;
"""

# ---------------------------------------------------------------------------
# Step 3: Create Attempts_part + pg_partman init
# ---------------------------------------------------------------------------

STEP3_EXTENSION_SQL = """
CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;
"""

# pg_partman create_parent (used when pg_partman works; Supabase may have issues)
STEP3_PARTMAN_SQL = """
SELECT create_parent(
    p_parent_table    => 'public.Attempts_part',
    p_control         => 'ts',
    p_interval        => '1 day',
    p_premake         => 3,
    p_start_partition => (now() - interval '35 days')::date::text
);
"""

# PostgreSQL does not allow generated columns as partition keys.
# Instead we use a plain TIMESTAMP NOT NULL column populated by a BEFORE INSERT
# trigger. The trigger fires before constraint checks and partition routing, so
# PostgreSQL correctly routes rows to the right child partition after ts is set.
STEP3_FUNCTION_SQL = """
CREATE OR REPLACE FUNCTION public.set_attempts_ts()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.ts := NEW.t1_timestamp::TIMESTAMP;
    RETURN NEW;
END;
$$;
"""

STEP3_SQL = """
CREATE TABLE IF NOT EXISTS public.Attempts_part (
    attempt_id                   SERIAL,
    market_id                    TEXT             NOT NULL REFERENCES Markets(market_id),
    parameter_set_id             INTEGER          NOT NULL REFERENCES ParameterSets(parameter_set_id),
    t1_timestamp                 TEXT             NOT NULL,
    first_leg_side               TEXT             NOT NULL,
    P1_points                    INTEGER          NOT NULL,
    reference_yes_points         INTEGER          NOT NULL,
    status                       TEXT             NOT NULL DEFAULT 'active',
    t2_timestamp                 TEXT,
    time_to_pair_seconds         DOUBLE PRECISION,
    time_remaining_at_start      DOUBLE PRECISION,
    time_remaining_at_completion DOUBLE PRECISION,
    actual_opposite_price        INTEGER,
    pair_cost_points             INTEGER,
    pair_profit_points           INTEGER,
    fail_reason                  TEXT,
    had_feed_gap                 INTEGER          DEFAULT 0,
    closest_approach_points      INTEGER,
    max_adverse_excursion_points INTEGER,
    yes_spread_entry_points      INTEGER,
    no_spread_entry_points       INTEGER,
    yes_spread_exit_points       INTEGER,
    no_spread_exit_points        INTEGER,
    delta_points                 INTEGER,
    S0_points                    INTEGER,
    stop_loss_threshold_points   INTEGER,
    ts                           TIMESTAMP        NOT NULL,
    PRIMARY KEY (attempt_id, ts)
) PARTITION BY RANGE (ts);
"""

STEP3_TRIGGER_SQL = """
CREATE OR REPLACE TRIGGER set_ts_before_insert
    BEFORE INSERT ON public.Attempts_part
    FOR EACH ROW EXECUTE FUNCTION public.set_attempts_ts();
"""


def _build_manual_partitions_sql(days_back: int = 35, days_forward: int = 4) -> str:
    """Build SQL to create daily partitions manually (fallback when pg_partman fails)."""
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=days_back)).date()
    end = (now + timedelta(days=days_forward)).date()
    parts = []
    d = start
    while d <= end:
        d_next = d + timedelta(days=1)
        lo = f"'{d.isoformat()} 00:00:00'::timestamp"
        hi = f"'{d_next.isoformat()} 00:00:00'::timestamp"
        name = f"Attempts_part_p{d.strftime('%Y%m%d')}"
        parts.append(
            f"CREATE TABLE IF NOT EXISTS public.{name} "
            f"PARTITION OF public.Attempts_part "
            f"FOR VALUES FROM ({lo}) TO ({hi})"
        )
        d = d_next
    # Default partition for any data outside the range
    parts.append(
        "CREATE TABLE IF NOT EXISTS public.Attempts_part_default "
        "PARTITION OF public.Attempts_part DEFAULT"
    )
    return ";\n".join(parts) + ";"


# ---------------------------------------------------------------------------
# Step 4: Data migration (batched)
# ---------------------------------------------------------------------------
STEP4_SELECT_COLUMNS = """
    attempt_id, market_id, parameter_set_id, t1_timestamp,
    first_leg_side, P1_points, reference_yes_points, status,
    t2_timestamp, time_to_pair_seconds, time_remaining_at_start,
    time_remaining_at_completion, actual_opposite_price,
    pair_cost_points, pair_profit_points, fail_reason, had_feed_gap,
    closest_approach_points, max_adverse_excursion_points,
    yes_spread_entry_points, no_spread_entry_points,
    yes_spread_exit_points, no_spread_exit_points,
    delta_points, S0_points, stop_loss_threshold_points
"""

# Include ts so rows route to correct partition (trigger can't change partition)
STEP4_INSERT_SQL = f"""
INSERT INTO Attempts_part (
    {STEP4_SELECT_COLUMNS},
    ts
)
SELECT
    {STEP4_SELECT_COLUMNS},
    t1_timestamp::TIMESTAMP AS ts
FROM Attempts
WHERE t1_timestamp::TIMESTAMP >= $1::timestamptz
  AND t1_timestamp::TIMESTAMP <  $2::timestamptz;
"""

# ---------------------------------------------------------------------------
# Step 5: Swap tables — individual statements for clarity and debuggability
# ---------------------------------------------------------------------------
STEP5_STATEMENTS = [
    # Reset sequence before rename so new inserts don't collide with migrated IDs
    "SELECT setval(pg_get_serial_sequence('Attempts_part', 'attempt_id'), (SELECT COALESCE(MAX(attempt_id), 0) FROM Attempts_part))",
    "ALTER TABLE Attempts      RENAME TO Attempts_old",
    "ALTER TABLE Attempts_part RENAME TO Attempts",
    # pg_partman stores parent_table as TEXT, must be updated after rename.
    # Unqualified — search_path (set in step3) resolves the correct schema.
    "UPDATE part_config SET parent_table = 'public.Attempts' WHERE parent_table = 'public.attempts_part'",
    # Recreate indexes — PG propagates to all existing and future child partitions
    "CREATE INDEX IF NOT EXISTS idx_attempts_market ON Attempts(market_id)",
    "CREATE INDEX IF NOT EXISTS idx_attempts_param_set ON Attempts(parameter_set_id)",
    "CREATE INDEX IF NOT EXISTS idx_attempts_optimizer ON Attempts(S0_points, delta_points, stop_loss_threshold_points, status, t1_timestamp)",
    """CREATE INDEX IF NOT EXISTS idx_attempts_optimize_params
    ON Attempts (delta_points, stop_loss_threshold_points, P1_points, time_remaining_at_start, t1_timestamp)
    WHERE status IN ('completed_paired', 'completed_failed')
      AND S0_points = 1
      AND time_remaining_at_start <= 900""",
    # Drop empty tables that will never be used
    "DROP TABLE IF EXISTS AttemptLifecycle",
    "DROP TABLE IF EXISTS Snapshots",
]

# ---------------------------------------------------------------------------
# Step 6: Schedule pg_cron
# ---------------------------------------------------------------------------
# The cron job runs in its own worker context (not our connection), so it needs
# a fully schema-qualified call. The schema is detected at runtime.
def build_step6_sql(partman_schema: str) -> str:
    return f"""
SELECT cron.schedule(
    'partman-maintenance',
    '0 * * * *',
    $$CALL {partman_schema}.run_maintenance_proc()$$
);
"""

# ---------------------------------------------------------------------------
# Step 7: Drop Attempts_old
# ---------------------------------------------------------------------------
STEP7_SQL = """
DROP TABLE IF EXISTS Attempts_old;
"""


async def run_step(conn, step_num: int, sql: str, *, in_transaction: bool = True) -> None:
    """Execute SQL for a step."""
    if in_transaction:
        async with conn.transaction():
            await conn.execute(sql)
    else:
        await conn.execute(sql)


async def get_partman_schema(conn) -> str:
    """Return the schema pg_partman is installed in (e.g. 'partman' or 'extensions')."""
    row = await conn.fetchrow("""
        SELECT n.nspname
        FROM pg_catalog.pg_extension e
        JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
        WHERE e.extname = 'pg_partman'
    """)
    if row is None:
        raise RuntimeError(
            "pg_partman extension not found. "
            "Enable it in Supabase Dashboard → Database → Extensions, then re-run."
        )
    return row["nspname"]


async def step1_create_index(conn) -> None:
    """Step 1: CREATE INDEX CONCURRENTLY (cannot run in transaction)."""
    print("[Step 1] Creating idx_attempts_optimize_params (CONCURRENTLY, may take a minute)...")
    await conn.execute(STEP1_SQL)
    print("  Done.")


async def step2_drop_redundant_indexes(conn) -> None:
    """Step 2: Drop redundant single-column indexes."""
    print("[Step 2] Dropping redundant indexes...")
    async with conn.transaction():
        await conn.execute(STEP2_SQL)
    print("  Done.")


async def step3_create_partitioned_table(conn) -> None:
    """Step 3: Create Attempts_part and initialize pg_partman."""
    print("[Step 3] Creating Attempts_part and initializing pg_partman...")
    async with conn.transaction():
        await conn.execute(STEP3_EXTENSION_SQL)

    # Detect whichever schema Supabase installed pg_partman into and put it on
    # the search_path for this connection. All later partman calls use
    # unqualified names so they work regardless of the actual schema name.
    partman_schema = await get_partman_schema(conn)
    print(f"  pg_partman schema: {partman_schema}")
    await conn.execute(f"SET search_path TO {partman_schema}, public")

    async with conn.transaction():
        await conn.execute(STEP3_FUNCTION_SQL)

    async with conn.transaction():
        await conn.execute(STEP3_SQL)
        await conn.execute(STEP3_TRIGGER_SQL)

    # Try pg_partman first; fall back to manual partitions if it can't find the table
    # (Supabase/pg_partman sometimes fails with "Unable to find given parent table")
    # PostgreSQL stores unquoted identifiers in lowercase; pg_partman looks up
    # by exact name in pg_tables, so use lowercase to match.
    partman_create = f"""
        SELECT {partman_schema}.create_parent(
            p_parent_table    => 'public.attempts_part',
            p_control         => 'ts',
            p_interval        => '1 day',
            p_premake         => 3,
            p_start_partition => (now() - interval '35 days')::date::text
        )
    """
    try:
        await conn.execute(partman_create)
        async with conn.transaction():
            await conn.execute(
                f"UPDATE {partman_schema}.part_config "
                "SET retention = '30 days', retention_keep_table = false "
                "WHERE parent_table = 'public.attempts_part'"
            )
        print("  pg_partman create_parent succeeded.")
    except Exception as e:
        err = str(e).lower()
        if "already" in err or "exists" in err:
            print("  pg_partman parent already registered, skipping.")
        elif "unable to find given parent table" in err or "parent table" in err:
            print("  pg_partman could not find table (Supabase quirk); creating partitions manually...")
            manual_sql = _build_manual_partitions_sql(days_back=35, days_forward=4)
            async with conn.transaction():
                for stmt in manual_sql.rstrip(";").split(";\n"):
                    await conn.execute(stmt)
            print("  Manual partitions created.")
        else:
            raise
    print("  Done.")


async def step4_migrate_data(
    conn,
    batch_days: int = 7,
    resume: bool = False,
    statement_timeout: str = "60min",
) -> None:
    """Step 4: Migrate last 30 days of data in batches."""
    print("[Step 4] Migrating last 30 days of data (batched)...")
    from datetime import datetime, timezone, timedelta

    await conn.execute("SET timezone TO 'UTC'")
    await conn.execute(f"SET statement_timeout = '{statement_timeout}'")
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=14)
    total_migrated = 0
    batch_num = 0

    while start < now:
        end = min(start + timedelta(days=batch_days), now)
        batch_num += 1

        if resume:
            old_count = await conn.fetchval(
                "SELECT COUNT(*) FROM Attempts "
                "WHERE t1_timestamp::TIMESTAMP >= $1::timestamptz "
                "AND t1_timestamp::TIMESTAMP < $2::timestamptz",
                start,
                end,
            )
            if old_count == 0:
                print(f"  Batch {batch_num}: skipped (no source data)")
                start = end
                continue
            new_count = await conn.fetchval(
                "SELECT COUNT(*) FROM Attempts_part "
                "WHERE ts >= $1::timestamptz AND ts < $2::timestamptz",
                start,
                end,
            )
            if old_count == new_count:
                print(f"  Batch {batch_num}: skipped (already migrated, {old_count:,} rows)")
                start = end
                continue

        result = await conn.execute(STEP4_INSERT_SQL, start, end)
        n = int(result.split()[-1]) if result else 0
        total_migrated += n
        print(f"  Batch {batch_num}: {n:,} rows ({start.date()} to {end.date()})")
        start = end

    print(f"  Total migrated: {total_migrated:,} rows.")

    # Verify
    old_count = await conn.fetchval(
        "SELECT COUNT(*) FROM Attempts WHERE t1_timestamp::TIMESTAMP >= now() - interval '30 days'"
    )
    new_count = await conn.fetchval("SELECT COUNT(*) FROM Attempts_part")
    print(f"  Verify: Attempts (14d)={old_count:,}, Attempts_part={new_count:,}")
    if old_count != new_count:
        print("  WARNING: Counts do not match. Abort before proceeding to step 5.")


async def step5_swap_tables(conn) -> None:
    """Step 5: Swap Attempts and Attempts_part."""
    print("[Step 5] Swapping tables (STOP THE BOT first!)...")
    async with conn.transaction():
        for sql in STEP5_STATEMENTS:
            await conn.execute(sql)
    print("  Done.")


async def step6_schedule_cron(conn) -> None:
    """Step 6: Schedule pg_cron maintenance."""
    print("[Step 6] Scheduling pg_cron hourly maintenance...")
    # The cron job runs in its own worker context, so we need the explicit schema.
    # get_partman_schema works because search_path was already set in step3.
    partman_schema = await get_partman_schema(conn)
    sql = build_step6_sql(partman_schema)
    try:
        await conn.execute(sql)
        print("  Done.")
    except Exception as e:
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            print("  Job already scheduled, skipping.")
        else:
            raise


async def step7_drop_old_table(conn) -> None:
    """Step 7: Drop Attempts_old (permanent data loss)."""
    print("[Step 7] Dropping Attempts_old...")
    async with conn.transaction():
        await conn.execute(STEP7_SQL)
    print("  Done.")


async def step8_register_migrations(conn) -> None:
    """Step 8: Mark migrations as applied so migrate.py won't re-run them."""
    import hashlib

    migrations_dir = Path(__file__).resolve().parent.parent / "migrations"
    files = [
        "008_attempts_analytics_index.sql",
        "009_attempts_partitioned.sql",
        "010_swap_attempts.sql",
        "011_drop_attempts_old.sql",
    ]
    print("[Step 8] Registering migrations in _migrations...")
    async with conn.transaction():
        for f in files:
            path = migrations_dir / f
            if path.exists():
                checksum = hashlib.sha256(path.read_bytes()).hexdigest()[:16]
                await conn.execute(
                    """
                    INSERT INTO _migrations (filename, checksum)
                    VALUES ($1, $2)
                    ON CONFLICT (filename) DO NOTHING
                    """,
                    f,
                    checksum,
                )
    print("  Done.")


def _parse_timeout_seconds(s: str) -> int:
    """Parse timeout string like '60min', '2h', '30min' to seconds."""
    s = s.strip().lower()
    if s.endswith("min"):
        return int(s[:-3]) * 60
    if s.endswith("h") or s.endswith("hr"):
        return int(s.rstrip("hr")) * 3600
    if s.endswith("s"):
        return int(s[:-1])
    return int(s)  # assume seconds


async def run(args) -> None:
    import asyncpg

    load_env_file()
    url = _resolve_db_url()

    # command_timeout must exceed statement_timeout for step 4 large batches
    cmd_timeout = _parse_timeout_seconds(args.statement_timeout) + 60
    pool = await asyncpg.create_pool(
        url, min_size=1, max_size=3, statement_cache_size=0, command_timeout=cmd_timeout
    )

    steps = [
        (1, step1_create_index, "Create analytics index (CONCURRENTLY)"),
        (2, step2_drop_redundant_indexes, "Drop redundant indexes"),
        (3, step3_create_partitioned_table, "Create Attempts_part + pg_partman"),
        (4, step4_migrate_data, "Migrate last 14 days of data"),
        (5, step5_swap_tables, "Swap tables (requires bot stopped)"),
        (6, step6_schedule_cron, "Schedule pg_cron"),
        (7, step7_drop_old_table, "Drop Attempts_old"),
        (8, step8_register_migrations, "Register migrations in _migrations"),
    ]

    start_from = args.step or 1
    if args.dry_run:
        print("Dry run — would execute steps:")
        for num, _, desc in steps:
            if num >= start_from and not (args.skip_drop and num == 7):
                print(f"  Step {num}: {desc}")
        return

    try:
        async with pool.acquire() as conn:
            for num, fn, desc in steps:
                if num < start_from:
                    continue
                if args.skip_drop and num == 7:
                    print("[Step 7] Skipped (--skip-drop).")
                    continue
                if num == 4:
                    await step4_migrate_data(
                        conn,
                        batch_days=args.batch_days,
                        resume=args.resume,
                        statement_timeout=args.statement_timeout,
                    )
                else:
                    await fn(conn)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run partitioned Attempts migration (replaces Supabase SQL Editor)"
    )
    parser.add_argument(
        "--step",
        type=int,
        default=None,
        help="Start from this step number (1-7)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would run without executing",
    )
    parser.add_argument(
        "--skip-drop",
        action="store_true",
        help="Skip step 7 (drop Attempts_old) — run it 24-48h later after verification",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=3,
        help="Days per batch for data migration (default: 3, use 1-2 if timing out)",
    )
    parser.add_argument(
        "--statement-timeout",
        type=str,
        default="60min",
        help="PostgreSQL statement_timeout for step 4 (e.g. 60min, 2h)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip batches already fully migrated (for resuming after timeout)",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
