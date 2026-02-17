#!/usr/bin/env python3
"""CLI for managing PostgreSQL database migrations.

Usage:
    python scripts/migrate.py apply                          # apply pending
    python scripts/migrate.py apply --dry-run                # preview only
    python scripts/migrate.py status                         # show status
    python scripts/migrate.py create "add_foo_column"        # scaffold new file

The database URL is resolved in order:
  1. --db-url flag
  2. DATABASE_URL environment variable
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402
from src.migration_runner import (  # noqa: E402
    get_migration_status,
    run_migrations,
    scaffold_migration,
)


def _resolve_db_url(args) -> str:
    """Return the PostgreSQL connection URL. Prefers session pooler for migrations."""
    url = (
        getattr(args, "db_url", None)
        or os.environ.get("DATABASE_URL_SESSION")
        or os.environ.get("DATABASE_URL")
    )
    if not url:
        print("Error: No database URL provided.")
        print("  Use --db-url or set DATABASE_URL_SESSION or DATABASE_URL.")
        print(f"  (Checked .env file: {Path(__file__).parent.parent / '.env'})")
        sys.exit(1)
    if "postgres" not in url.lower():
        print("Error: Migrations only support PostgreSQL. Got:", url[:30])
        sys.exit(1)
    # Debug: show what URL we're using (mask password)
    if "://" in url and "@" in url:
        parts = url.split("@", 1)
        if ":" in parts[0]:
            scheme_user = parts[0].split(":")
            if len(scheme_user) >= 3:
                masked = f"{scheme_user[0]}:{scheme_user[1]}:***@{parts[1]}"
            else:
                masked = f"{parts[0].split(':')[0]}:***@{parts[1]}"
        else:
            masked = url
        print(f"Connecting to: {masked}")
        # Parse and print components for debugging
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            netloc = parsed.netloc
            if "@" in netloc:
                netloc = netloc.split("@", 1)[1]  # host:port after @
            if ":" in netloc:
                host, port = netloc.rsplit(":", 1)
            else:
                host, port = netloc, "5432"
            print(f"  host={host!r} port={port!r} database={parsed.path.strip('/')!r}")
        except Exception:
            pass
    return url


async def cmd_apply(args) -> None:
    """Apply pending migrations."""
    import asyncpg

    url = _resolve_db_url(args)
    session_url = os.environ.get("DATABASE_URL_SESSION")
    urls_to_try = [url]
    if session_url and session_url != url:
        urls_to_try.append(session_url)

    pool = None
    last_error = None
    for try_url in urls_to_try:
        for attempt in range(3):
            try:
                pool = await asyncpg.create_pool(
                    try_url, min_size=1, max_size=3, statement_cache_size=0
                )
                break
            except Exception as e:
                last_error = e
                if attempt < 2:
                    wait = 10 * (attempt + 1)
                    print(f"  Retrying in {wait}s... (attempt {attempt + 1}/3)")
                    await asyncio.sleep(wait)
                elif try_url != urls_to_try[-1]:
                    print("  Trying session pooler instead...")
                    break
                else:
                    raise
        if pool is not None:
            break

    if pool is None:
        raise last_error or RuntimeError("Failed to create pool")

    try:
        applied = await run_migrations(pool, dry_run=args.dry_run)
        if applied:
            prefix = "[DRY RUN] Would apply" if args.dry_run else "Applied"
            for name in applied:
                print(f"  {prefix}: {name}")
            print(f"\n{len(applied)} migration(s) {'would be applied' if args.dry_run else 'applied'}.")
        else:
            print("Database is up to date — nothing to apply.")
    finally:
        await pool.close()


async def cmd_status(args) -> None:
    """Show migration status."""
    import asyncpg

    url = _resolve_db_url(args)
    pool = await asyncpg.create_pool(url, min_size=1, max_size=3,
                                     statement_cache_size=0)

    try:
        status = await get_migration_status(pool)

        print("=== Applied Migrations ===")
        if status["applied"]:
            for m in status["applied"]:
                ts = m["applied_at"].strftime("%Y-%m-%d %H:%M:%S") if m["applied_at"] else "?"
                print(f"  {m['filename']}  (applied {ts})")
        else:
            print("  (none)")

        print("\n=== Pending Migrations ===")
        if status["pending"]:
            for name in status["pending"]:
                print(f"  {name}")
        else:
            print("  (none — up to date)")

        if status["changed"]:
            print("\n=== Changed Since Applied (WARNING) ===")
            for name in status["changed"]:
                print(f"  {name}  ← file has been modified!")
    finally:
        await pool.close()


def cmd_create(args) -> None:
    """Scaffold a new migration file."""
    path = scaffold_migration(args.description)
    print(f"Created: {path}")
    print(f"Edit this file, then run: python scripts/migrate.py apply")


def main():
    # Load .env file if it exists
    load_env_file()
    
    parser = argparse.ArgumentParser(
        description="Manage PostgreSQL database migrations"
    )
    parser.add_argument(
        "--db-url", default=None,
        help="PostgreSQL connection URL (overrides DATABASE_URL env var)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- apply ---
    apply_parser = subparsers.add_parser("apply", help="Apply pending migrations")
    apply_parser.add_argument(
        "--dry-run", action="store_true",
        help="Show what would be applied without running it",
    )

    # --- status ---
    subparsers.add_parser("status", help="Show migration status")

    # --- create ---
    create_parser = subparsers.add_parser("create", help="Scaffold a new migration file")
    create_parser.add_argument(
        "description",
        help="Short description for the migration (e.g. 'add_foo_column')",
    )

    args = parser.parse_args()

    if args.command == "apply":
        asyncio.run(cmd_apply(args))
    elif args.command == "status":
        asyncio.run(cmd_status(args))
    elif args.command == "create":
        cmd_create(args)


if __name__ == "__main__":
    main()

