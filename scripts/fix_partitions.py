#!/usr/bin/env python3
"""Fix partitioned Attempts table — relocate rows from default partition,
create missing partitions, drop old ones, and fix cron jobs.

Usage:
    python scripts/fix_partitions.py
    python scripts/fix_partitions.py --step 3      # start from step 3
    python scripts/fix_partitions.py --batch 50000  # smaller batches
    python scripts/fix_partitions.py --dry-run      # show what would run
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402


def _resolve_db_url() -> str:
    url = os.environ.get("DATABASE_URL_SESSION") or os.environ.get("DATABASE_URL")
    if not url:
        print("Error: No database URL. Set DATABASE_URL_SESSION or DATABASE_URL.")
        sys.exit(1)
    return url


async def _exec(conn, sql: str, label: str, timeout: int = 300):
    print(f"  Running: {label}")
    await conn.execute(f"SET statement_timeout = '{timeout}s'")
    await conn.execute(sql)
    print(f"  Done: {label}")


async def run(db_url: str, start_step: int = 1, batch_size: int = 100_000, dry_run: bool = False):
    import asyncpg

    print(f"\n{'=' * 80}")
    print("  FIX PARTITIONS")
    print(f"{'=' * 80}\n")

    conn = await asyncpg.connect(db_url, statement_cache_size=0,
                                  command_timeout=600)

    # ------------------------------------------------------------------
    # Step 1: Delete broken cron jobs
    # ------------------------------------------------------------------
    if start_step <= 1:
        print("[Step 1] Deleting broken cron jobs (2 and 3)...")
        if dry_run:
            print("  (dry run)")
        else:
            jobs = await conn.fetch("SELECT jobid FROM cron.job WHERE jobid IN (2, 3)")
            for row in jobs:
                await conn.execute("SELECT cron.unschedule($1)", row["jobid"])
                print(f"  Unscheduled job {row['jobid']}")
            if not jobs:
                print("  Jobs 2 and 3 already removed.")

    # ------------------------------------------------------------------
    # Step 2: Detach default partition
    # ------------------------------------------------------------------
    if start_step <= 2:
        print("\n[Step 2] Detaching default partition...")
        if dry_run:
            print("  (dry run)")
        else:
            try:
                await _exec(conn,
                    "ALTER TABLE public.attempts DETACH PARTITION public.attempts_part_default",
                    "detach default")
            except Exception as e:
                if "is not a partition" in str(e) or "does not exist" in str(e):
                    print(f"  Skipped (already detached or doesn't exist): {e}")
                else:
                    raise

    # ------------------------------------------------------------------
    # Step 3: Create missing partitions
    # ------------------------------------------------------------------
    if start_step <= 3:
        print("\n[Step 3] Creating missing partitions (Mar 9 - Mar 17)...")
        if dry_run:
            print("  (dry run)")
        else:
            await _exec(conn, """
                DO $$
                DECLARE
                    d date;
                    tname text;
                BEGIN
                    FOR d IN SELECT generate_series(
                        '2026-03-09'::date,
                        '2026-03-17'::date,
                        '1 day'::interval
                    )::date LOOP
                        tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
                        EXECUTE format(
                            'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.attempts FOR VALUES FROM (%L) TO (%L)',
                            tname, d::timestamp, (d + 1)::timestamp
                        );
                        RAISE NOTICE 'Created %', tname;
                    END LOOP;
                END $$;
            """, "create partitions")

    # ------------------------------------------------------------------
    # Step 4: Move rows from default to correct partitions (batched)
    # ------------------------------------------------------------------
    if start_step <= 4:
        print(f"\n[Step 4] Moving rows from default partition (batch size: {batch_size:,})...")
        if dry_run:
            print("  (dry run)")
        else:
            total_moved = 0
            while True:
                # Fresh connection each batch to avoid timeout disconnects
                try:
                    await conn.close()
                except Exception:
                    pass
                conn = await asyncpg.connect(db_url, statement_cache_size=0,
                                              command_timeout=600)
                await conn.execute("SET statement_timeout = '300s'")

                remaining = await conn.fetchval(
                    "SELECT COUNT(*) FROM public.attempts_part_default"
                )
                if remaining == 0:
                    break
                print(f"  Remaining in default: {remaining:,}")

                # Delete from default, insert into partitioned parent, skip duplicates
                result = await conn.execute(f"""
                    WITH moved AS (
                        DELETE FROM public.attempts_part_default
                        WHERE ctid IN (
                            SELECT ctid FROM public.attempts_part_default
                            LIMIT {batch_size}
                        )
                        RETURNING *
                    )
                    INSERT INTO public.attempts
                    SELECT * FROM moved
                    ON CONFLICT (attempt_id, ts) DO NOTHING
                """)
                moved = int(result.split()[-1]) if result else 0

                total_moved += moved
                print(f"  Moved batch: {moved:,} rows (total: {total_moved:,})")

            print(f"  All rows moved: {total_moved:,} total")

    # ------------------------------------------------------------------
    # Step 5: Re-attach default partition
    # ------------------------------------------------------------------
    if start_step <= 5:
        print("\n[Step 5] Re-attaching default partition...")
        if dry_run:
            print("  (dry run)")
        else:
            try:
                await _exec(conn,
                    "ALTER TABLE public.attempts ATTACH PARTITION public.attempts_part_default DEFAULT",
                    "attach default")
            except Exception as e:
                if "already a partition" in str(e):
                    print(f"  Already attached: {e}")
                else:
                    raise

    # ------------------------------------------------------------------
    # Step 6: Drop old partitions (> 30 days)
    # ------------------------------------------------------------------
    if start_step <= 6:
        print("\n[Step 6] Dropping partitions older than 30 days...")
        if dry_run:
            print("  (dry run)")
        else:
            await _exec(conn, """
                DO $$
                DECLARE
                    d date;
                    tname text;
                BEGIN
                    FOR d IN SELECT generate_series(
                        '2026-02-04'::date,
                        (CURRENT_DATE - 30)::date,
                        '1 day'::interval
                    )::date LOOP
                        tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
                        EXECUTE format('DROP TABLE IF EXISTS public.%I', tname);
                        RAISE NOTICE 'Dropped %', tname;
                    END LOOP;
                END $$;
            """, "drop old partitions")

    # ------------------------------------------------------------------
    # Step 7: Fix pg_partman config
    # ------------------------------------------------------------------
    if start_step <= 7:
        print("\n[Step 7] Fixing pg_partman config...")
        if dry_run:
            print("  (dry run)")
        else:
            existing = await conn.fetch(
                "SELECT parent_table FROM extensions.part_config"
            )
            print(f"  Current config: {[r['parent_table'] for r in existing]}")

            await conn.execute("""
                DELETE FROM extensions.part_config
                WHERE parent_table IN ('public.attempts_part', 'public.attempts')
            """)

            try:
                await conn.execute("""
                    SELECT extensions.create_parent(
                        p_parent_table => 'public.attempts',
                        p_control      => 'ts',
                        p_interval     => '1 day',
                        p_premake      => 3
                    )
                """)
                await conn.execute("""
                    UPDATE extensions.part_config
                    SET retention = '30 days', retention_keep_table = false
                    WHERE parent_table = 'public.attempts'
                """)
                print("  Registered public.attempts with pg_partman")
            except Exception as e:
                print(f"  pg_partman registration failed: {e}")
                print("  Will use manual cron jobs instead (Step 8)")

    # ------------------------------------------------------------------
    # Step 8: Test maintenance and schedule cron fallback if needed
    # ------------------------------------------------------------------
    if start_step <= 8:
        print("\n[Step 8] Testing maintenance and scheduling cron...")
        if dry_run:
            print("  (dry run)")
        else:
            try:
                await conn.execute("CALL extensions.run_maintenance_proc()")
                print("  run_maintenance_proc() succeeded — Job 1 will handle ongoing maintenance")
            except Exception as e:
                print(f"  run_maintenance_proc() failed: {e}")
                print("  Scheduling manual cron jobs as fallback...")

                await conn.execute("""
                    SELECT cron.schedule(
                        'create-partitions',
                        '0 23 * * *',
                        $cron$
                        DO $inner$
                        DECLARE
                            d date;
                            tname text;
                        BEGIN
                            FOR d IN SELECT generate_series(
                                (CURRENT_DATE + 1)::timestamp,
                                (CURRENT_DATE + 3)::timestamp,
                                '1 day'::interval
                            )::date LOOP
                                tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
                                EXECUTE format(
                                    'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.attempts FOR VALUES FROM (%L) TO (%L)',
                                    tname, d::timestamp, (d + 1)::timestamp
                                );
                            END LOOP;
                        END $inner$
                        $cron$
                    )
                """)

                await conn.execute("""
                    SELECT cron.schedule(
                        'drop-old-partitions',
                        '0 23 * * *',
                        $cron$
                        DO $inner$
                        DECLARE
                            d date;
                            tname text;
                        BEGIN
                            FOR d IN SELECT generate_series(
                                (CURRENT_DATE - 35)::date,
                                (CURRENT_DATE - 30)::date,
                                '1 day'::interval
                            )::date LOOP
                                tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
                                EXECUTE format('DROP TABLE IF EXISTS public.%I', tname);
                            END LOOP;
                        END $inner$
                        $cron$
                    )
                """)
                print("  Manual cron jobs scheduled")

    # ------------------------------------------------------------------
    # Verify
    # ------------------------------------------------------------------
    print(f"\n{'=' * 80}")
    print("  VERIFICATION")
    print(f"{'=' * 80}\n")

    partitions = await conn.fetch("""
        SELECT tablename FROM pg_tables
        WHERE tablename LIKE 'attempts_part%'
        ORDER BY tablename
    """)
    print(f"  Partitions ({len(partitions)}):")
    for r in partitions:
        print(f"    {r['tablename']}")

    default_count = await conn.fetchval(
        "SELECT COUNT(*) FROM public.attempts_part_default"
    )
    print(f"\n  Rows in default partition: {default_count:,}")

    jobs = await conn.fetch("SELECT jobid, schedule, command, active FROM cron.job")
    print(f"\n  Cron jobs ({len(jobs)}):")
    for j in jobs:
        cmd_preview = j["command"][:60].replace("\n", " ")
        print(f"    Job {j['jobid']}: {j['schedule']} | active={j['active']} | {cmd_preview}...")

    await conn.close()
    print(f"\n{'=' * 80}")
    print("  Done.")
    print(f"{'=' * 80}\n")


def main():
    load_env_file()
    parser = argparse.ArgumentParser(description="Fix partition issues")
    parser.add_argument("--step", type=int, default=1, help="Start from step N")
    parser.add_argument("--batch", type=int, default=100_000, help="Batch size for row moves")
    parser.add_argument("--dry-run", action="store_true", help="Show what would run")
    args = parser.parse_args()

    db_url = _resolve_db_url()
    asyncio.run(run(db_url, start_step=args.step, batch_size=args.batch, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
