"""Lightweight SQL migration runner for PostgreSQL.

Tracks applied migrations in a ``_migrations`` table and applies pending
``.sql`` files from the ``migrations/`` directory in filename order.

Usage from code::

    from src.migration_runner import run_migrations
    await run_migrations(pool)          # asyncpg Pool

Usage from CLI::

    python scripts/migrate.py apply
    python scripts/migrate.py status
    python scripts/migrate.py create "add_foo_column"
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Default directory where .sql migration files live
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

# Tracking table name
TRACKING_TABLE = "_migrations"


async def _ensure_tracking_table(conn) -> None:
    """Create the tracking table if it doesn't exist."""
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TRACKING_TABLE} (
            id              SERIAL PRIMARY KEY,
            filename        TEXT NOT NULL UNIQUE,
            applied_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            checksum        TEXT
        )
    """)


async def _get_applied(conn) -> set[str]:
    """Return the set of already-applied migration filenames."""
    rows = await conn.fetch(
        f"SELECT filename FROM {TRACKING_TABLE} ORDER BY filename"
    )
    return {r["filename"] for r in rows}


def _discover_migrations(migrations_dir: Path | None = None) -> list[Path]:
    """Return sorted list of .sql files in the migrations directory."""
    d = migrations_dir or MIGRATIONS_DIR
    if not d.exists():
        logger.warning("Migrations directory not found: %s", d)
        return []
    files = sorted(d.glob("*.sql"))
    return files


def _file_checksum(path: Path) -> str:
    """Simple hash of file contents for change detection."""
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()[:16]


async def run_migrations(
    pool,
    migrations_dir: Path | None = None,
    *,
    dry_run: bool = False,
) -> list[str]:
    """Apply any pending migrations and return filenames that were applied.

    Args:
        pool: An ``asyncpg.Pool`` instance.
        migrations_dir: Override the default ``migrations/`` directory.
        dry_run: If True, report pending migrations without applying them.

    Returns:
        List of filenames that were (or would be) applied.
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _ensure_tracking_table(conn)
            applied = await _get_applied(conn)

    all_files = _discover_migrations(migrations_dir)
    pending = [f for f in all_files if f.name not in applied]

    if not pending:
        logger.info("Database is up to date — no pending migrations")
        return []

    if dry_run:
        for f in pending:
            logger.info("[DRY RUN] Would apply: %s", f.name)
        return [f.name for f in pending]

    applied_names: list[str] = []
    for migration_file in pending:
        sql = migration_file.read_text(encoding="utf-8")
        checksum = _file_checksum(migration_file)

        # Strip BEGIN/COMMIT so we can wrap in our own transaction
        sql_body = re.sub(r'^\s*BEGIN\s*;', '', sql, flags=re.IGNORECASE)
        sql_body = re.sub(r'COMMIT\s*;\s*$', '', sql_body, flags=re.IGNORECASE).strip()

        logger.info("Applying migration: %s", migration_file.name)
        async with pool.acquire() as conn:
            async with conn.transaction():
                if sql_body:
                    await conn.execute(sql_body)
                await conn.execute(
                    f"INSERT INTO {TRACKING_TABLE} (filename, checksum) VALUES ($1, $2)",
                    migration_file.name,
                    checksum,
                )
        logger.info("Applied migration: %s", migration_file.name)
        applied_names.append(migration_file.name)

    return applied_names


async def get_migration_status(
    pool,
    migrations_dir: Path | None = None,
) -> dict:
    """Return migration status for reporting.

    Returns:
        dict with keys:
            applied  — list of {filename, applied_at, checksum}
            pending  — list of filenames not yet applied
            changed  — list of filenames whose checksum differs from applied
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _ensure_tracking_table(conn)
            rows = await conn.fetch(
                f"SELECT filename, applied_at, checksum FROM {TRACKING_TABLE} ORDER BY filename"
            )

    applied_map = {r["filename"]: dict(r) for r in rows}
    all_files = _discover_migrations(migrations_dir)

    pending = []
    changed = []
    for f in all_files:
        if f.name not in applied_map:
            pending.append(f.name)
        else:
            current_checksum = _file_checksum(f)
            if applied_map[f.name]["checksum"] != current_checksum:
                changed.append(f.name)

    return {
        "applied": [dict(r) for r in rows],
        "pending": pending,
        "changed": changed,
    }


def scaffold_migration(description: str, migrations_dir: Path | None = None) -> Path:
    """Create a new empty migration file with the next sequence number.

    Args:
        description: Short description (used in filename).
        migrations_dir: Override the default ``migrations/`` directory.

    Returns:
        Path to the created file.
    """
    d = migrations_dir or MIGRATIONS_DIR
    d.mkdir(parents=True, exist_ok=True)

    existing = sorted(d.glob("*.sql"))
    if existing:
        # Extract number from last filename like "003_something.sql"
        last_num = int(existing[-1].name.split("_")[0])
    else:
        last_num = 0

    next_num = last_num + 1
    safe_desc = description.lower().replace(" ", "_").replace("-", "_")
    filename = f"{next_num:03d}_{safe_desc}.sql"
    filepath = d / filename

    filepath.write_text(
        f"-- {filename}\n"
        f"-- Migration: {description}\n"
        f"-- Created: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"\n"
        f"BEGIN;\n"
        f"\n"
        f"-- Your SQL here\n"
        f"\n"
        f"COMMIT;\n",
        encoding="utf-8",
    )

    logger.info("Created migration: %s", filepath)
    return filepath

