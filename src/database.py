"""Database layer supporting PostgreSQL (primary) and SQLite (fallback).

PostgreSQL schema is managed by SQL migration files in ``migrations/``.
SQLite uses an inline schema for local-dev convenience.

An ``asyncio.Lock`` serialises SQLite writes; PostgreSQL uses a connection
pool and needs no external lock.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import (
    Attempt, AttemptStatus, LifecycleRecord, MarketInfo, ParameterSet,
    Side, Snapshot,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Schema — SQLite (local dev only — PG uses migrations/)
# ---------------------------------------------------------------------------

SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS ParameterSets (
    parameter_set_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name                    TEXT    NOT NULL,
    S0_points               INTEGER NOT NULL,
    delta_points            INTEGER NOT NULL,
    PairCap_points          INTEGER NOT NULL,
    trigger_rule            TEXT    NOT NULL,
    reference_price_source  TEXT    NOT NULL,
    tie_break_rule          TEXT    DEFAULT 'distance_then_yes',
    sampling_mode           TEXT,
    cycle_interval_seconds  REAL,
    cycles_per_market       INTEGER,
    feed_gap_threshold_seconds REAL,
    stop_loss_threshold_points INTEGER,
    created_at              TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS Markets (
    market_id               TEXT PRIMARY KEY,
    crypto_asset            TEXT    NOT NULL,
    condition_id            TEXT    NOT NULL,
    yes_token_id            TEXT    NOT NULL,
    no_token_id             TEXT    NOT NULL,
    start_time              TEXT    NOT NULL,
    settlement_time         TEXT    NOT NULL,
    actual_settlement_time  TEXT,
    tick_size_points        INTEGER NOT NULL,
    parameter_set_id        INTEGER REFERENCES ParameterSets(parameter_set_id),
    total_attempts          INTEGER DEFAULT 0,
    total_pairs             INTEGER DEFAULT 0,
    total_failed            INTEGER DEFAULT 0,
    settlement_failures     INTEGER DEFAULT 0,
    pair_rate               REAL,
    avg_time_to_pair        REAL,
    median_time_to_pair     REAL,
    max_concurrent_attempts INTEGER DEFAULT 0,
    total_cycles_run        INTEGER DEFAULT 0,
    cycle_interval_seconds  REAL,
    time_remaining_at_start REAL,
    anomaly_count           INTEGER DEFAULT 0,
    notes                   TEXT
);

CREATE TABLE IF NOT EXISTS Attempts (
    attempt_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id               TEXT    NOT NULL REFERENCES Markets(market_id),
    parameter_set_id        INTEGER NOT NULL REFERENCES ParameterSets(parameter_set_id),
    cycle_number            INTEGER NOT NULL,
    t1_timestamp            TEXT    NOT NULL,
    first_leg_side          TEXT    NOT NULL,
    P1_points               INTEGER NOT NULL,
    reference_yes_points    INTEGER NOT NULL,
    reference_no_points     INTEGER NOT NULL,
    opposite_side           TEXT    NOT NULL,
    opposite_trigger_points INTEGER NOT NULL,
    opposite_max_points     INTEGER NOT NULL,
    status                  TEXT    NOT NULL DEFAULT 'active',
    t2_timestamp            TEXT,
    t2_cycle_number         INTEGER,
    time_to_pair_seconds    REAL,
    time_remaining_at_start REAL,
    time_remaining_at_completion REAL,
    actual_opposite_price   INTEGER,
    pair_cost_points        INTEGER,
    pair_profit_points      INTEGER,
    fail_reason             TEXT,
    had_feed_gap            INTEGER DEFAULT 0,
    closest_approach_points INTEGER,
    closest_approach_timestamp TEXT,
    closest_approach_cycle_number INTEGER,
    max_adverse_excursion_points INTEGER,
    mae_timestamp           TEXT,
    mae_cycle_number        INTEGER,
    time_remaining_bucket   TEXT,
    yes_spread_entry_points INTEGER,
    no_spread_entry_points  INTEGER,
    yes_spread_exit_points  INTEGER,
    no_spread_exit_points   INTEGER,
    delta_points            INTEGER,
    S0_points               INTEGER,
    stop_loss_threshold_points INTEGER,
    stop_loss_price_points  INTEGER
);

CREATE TABLE IF NOT EXISTS Snapshots (
    snapshot_id             INTEGER PRIMARY KEY AUTOINCREMENT,
    market_id               TEXT    NOT NULL REFERENCES Markets(market_id),
    cycle_number            INTEGER NOT NULL,
    timestamp               TEXT    NOT NULL,
    yes_bid_points          INTEGER,
    yes_ask_points          INTEGER,
    no_bid_points           INTEGER,
    no_ask_points           INTEGER,
    yes_last_trade_points   INTEGER,
    no_last_trade_points    INTEGER,
    time_remaining          REAL,
    active_attempts_count   INTEGER DEFAULT 0,
    anomaly_flag            INTEGER DEFAULT 0,
    yes_period_low_ask_points INTEGER,
    no_period_low_ask_points  INTEGER
);

CREATE TABLE IF NOT EXISTS AttemptLifecycle (
    lifecycle_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    attempt_id              INTEGER NOT NULL REFERENCES Attempts(attempt_id),
    cycle_number            INTEGER NOT NULL,
    timestamp               TEXT    NOT NULL,
    opposite_ask_points     INTEGER,
    distance_to_trigger     INTEGER,
    closest_approach_so_far INTEGER
);
"""

# Columns that may be missing in older SQLite databases
_SQLITE_MIGRATION_COLUMNS = [
    "closest_approach_points INTEGER",
    "closest_approach_timestamp TEXT",
    "closest_approach_cycle_number INTEGER",
    "max_adverse_excursion_points INTEGER",
    "mae_timestamp TEXT",
    "mae_cycle_number INTEGER",
    "time_remaining_bucket TEXT",
    "yes_spread_entry_points INTEGER",
    "no_spread_entry_points INTEGER",
    "yes_spread_exit_points INTEGER",
    "no_spread_exit_points INTEGER",
    "delta_points INTEGER",
    "S0_points INTEGER",
    "stop_loss_threshold_points INTEGER",
    "stop_loss_price_points INTEGER",
]

# Columns that may be missing on ParameterSets in older SQLite databases
_SQLITE_PS_MIGRATION_COLUMNS = [
    "stop_loss_threshold_points INTEGER",
]


# ---------------------------------------------------------------------------
# SQL helper
# ---------------------------------------------------------------------------

def _q(sql: str) -> str:
    """Convert ``?`` placeholders to ``$1, $2, …`` for PostgreSQL."""
    parts = sql.split("?")
    if len(parts) <= 1:
        return sql
    result = parts[0]
    for i, part in enumerate(parts[1:], 1):
        result += f"${i}" + part
    return result


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    """Async database manager — PostgreSQL (asyncpg) or SQLite (aiosqlite).

    PostgreSQL schema is managed by migration files in ``migrations/``.
    SQLite uses an inline schema (for local dev).
    """

    def __init__(
        self,
        database_url: Optional[str] = None,
        db_path: str = "data/measurements.db",
    ):
        self._database_url = database_url
        self._db_path = db_path
        self._is_postgres: bool = bool(
            database_url
            and ("postgres" in database_url.lower())
        )

        # SQLite state
        self._db = None                    # aiosqlite.Connection
        self._write_lock = asyncio.Lock()

        # PostgreSQL state
        self._pool = None                  # asyncpg.Pool

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Create tables / run migrations."""
        if self._is_postgres:
            import asyncpg                     # lazy import
            from .migration_runner import run_migrations

            dsn = self._database_url
            self._pool = await asyncpg.create_pool(
                dsn, min_size=2, max_size=10,
                statement_cache_size=0,
            )
            applied = await run_migrations(self._pool)
            if applied:
                logger.info(
                    "Applied %d migration(s): %s",
                    len(applied), ", ".join(applied),
                )
            logger.info("PostgreSQL database initialized")
        else:
            import aiosqlite                   # lazy import

            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
            self._db = await aiosqlite.connect(self._db_path)
            self._db.row_factory = aiosqlite.Row
            await self._db.executescript(SQLITE_SCHEMA)
            await self._db.commit()
            await self._run_sqlite_migrations()
            logger.info("SQLite database initialized at %s", self._db_path)

    async def close(self) -> None:
        """Close the database connection / pool."""
        if self._is_postgres and self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed")
        elif self._db:
            await self._db.close()
            self._db = None
            logger.info("SQLite connection closed")

    # ------------------------------------------------------------------
    # SQLite migrations (add missing columns to older databases)
    # ------------------------------------------------------------------

    async def _run_sqlite_migrations(self) -> None:
        """Add columns that may be missing in an older SQLite file."""
        assert self._db is not None
        for col_def in _SQLITE_MIGRATION_COLUMNS:
            try:
                await self._db.execute(
                    f"ALTER TABLE Attempts ADD COLUMN {col_def}"
                )
                await self._db.commit()
            except Exception:
                pass  # column already exists
        for col_def in _SQLITE_PS_MIGRATION_COLUMNS:
            try:
                await self._db.execute(
                    f"ALTER TABLE ParameterSets ADD COLUMN {col_def}"
                )
                await self._db.commit()
            except Exception:
                pass  # column already exists

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _execute(self, sql: str, params: tuple = ()) -> None:
        """Execute a statement with no return value."""
        if self._is_postgres:
            async with self._pool.acquire() as conn:
                await conn.execute(_q(sql), *params)
        else:
            async with self._write_lock:
                await self._db.execute(sql, params)
                await self._db.commit()

    async def _insert_returning_id(
        self, sql: str, params: tuple, id_column: str,
    ) -> int:
        """INSERT … and return the auto-generated ID."""
        if self._is_postgres:
            pg_sql = _q(sql) + f" RETURNING {id_column}"
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(pg_sql, *params)
                return row[id_column]
        else:
            async with self._write_lock:
                cursor = await self._db.execute(sql, params)
                await self._db.commit()
                return cursor.lastrowid

    async def _executemany(self, sql: str, params_list: list[tuple]) -> None:
        """Execute a statement for many parameter tuples."""
        if not params_list:
            return
        if self._is_postgres:
            async with self._pool.acquire() as conn:
                await conn.executemany(_q(sql), params_list)
        else:
            async with self._write_lock:
                await self._db.executemany(sql, params_list)
                await self._db.commit()

    # ------------------------------------------------------------------
    # ParameterSets
    # ------------------------------------------------------------------

    async def insert_parameter_set(
        self,
        ps: ParameterSet,
        sampling_mode: str,
        cycle_interval: float,
        cycles_per_market: int,
        feed_gap_threshold: float,
    ) -> int:
        """Insert a parameter set and return its auto-generated ID."""
        sql = """INSERT INTO ParameterSets
                 (name, S0_points, delta_points, PairCap_points, trigger_rule,
                  reference_price_source, sampling_mode, cycle_interval_seconds,
                  cycles_per_market, feed_gap_threshold_seconds,
                  stop_loss_threshold_points, created_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        params = (
            ps.name, ps.S0_points, ps.delta_points, ps.pair_cap_points,
            ps.trigger_rule.value, ps.reference_price_source.value,
            sampling_mode, cycle_interval, cycles_per_market,
            feed_gap_threshold,
            ps.stop_loss_threshold_points,
            datetime.now(timezone.utc).isoformat(),
        )
        ps.parameter_set_id = await self._insert_returning_id(
            sql, params, "parameter_set_id",
        )
        logger.info(
            "Inserted parameter set '%s' with id=%d",
            ps.name, ps.parameter_set_id,
        )
        return ps.parameter_set_id

    # ------------------------------------------------------------------
    # Markets
    # ------------------------------------------------------------------

    async def insert_market(
        self,
        market_info: MarketInfo,
        parameter_set_id: int,
        start_time: datetime,
        time_remaining: float,
        cycle_interval: float,
    ) -> None:
        """Insert (or upsert) a new market record."""
        params = (
            market_info.market_slug, market_info.crypto_asset,
            market_info.condition_id, market_info.yes_token_id,
            market_info.no_token_id, start_time.isoformat(),
            market_info.settlement_time.isoformat(),
            market_info.tick_size_points, parameter_set_id,
            time_remaining, cycle_interval,
        )

        if self._is_postgres:
            sql = """INSERT INTO Markets
                     (market_id, crypto_asset, condition_id, yes_token_id,
                      no_token_id, start_time, settlement_time,
                      tick_size_points, parameter_set_id,
                      time_remaining_at_start, cycle_interval_seconds)
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
                     ON CONFLICT (market_id) DO UPDATE SET
                        crypto_asset = EXCLUDED.crypto_asset,
                        condition_id = EXCLUDED.condition_id,
                        yes_token_id = EXCLUDED.yes_token_id,
                        no_token_id  = EXCLUDED.no_token_id,
                        start_time   = EXCLUDED.start_time,
                        settlement_time = EXCLUDED.settlement_time,
                        tick_size_points = EXCLUDED.tick_size_points,
                        parameter_set_id = EXCLUDED.parameter_set_id,
                        time_remaining_at_start = EXCLUDED.time_remaining_at_start,
                        cycle_interval_seconds  = EXCLUDED.cycle_interval_seconds"""
            async with self._pool.acquire() as conn:
                await conn.execute(sql, *params)
        else:
            sql = """INSERT OR REPLACE INTO Markets
                     (market_id, crypto_asset, condition_id, yes_token_id,
                      no_token_id, start_time, settlement_time,
                      tick_size_points, parameter_set_id,
                      time_remaining_at_start, cycle_interval_seconds)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            async with self._write_lock:
                await self._db.execute(sql, params)
                await self._db.commit()

        logger.debug("Inserted market %s", market_info.market_slug)

    # ------------------------------------------------------------------
    # Attempts — single-row (kept for backward compat / rare one-offs)
    # ------------------------------------------------------------------

    async def insert_attempt(self, attempt: Attempt) -> int:
        """Insert a new attempt and return its auto-generated ID."""
        sql = """INSERT INTO Attempts
                 (market_id, parameter_set_id, cycle_number, t1_timestamp,
                  first_leg_side, P1_points, reference_yes_points,
                  reference_no_points, opposite_side,
                  opposite_trigger_points, opposite_max_points,
                  status, time_remaining_at_start,
                  time_remaining_bucket,
                  yes_spread_entry_points, no_spread_entry_points,
                  delta_points, S0_points,
                  stop_loss_threshold_points, stop_loss_price_points)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        params = self._attempt_insert_params(attempt)
        attempt.attempt_id = await self._insert_returning_id(
            sql, params, "attempt_id",
        )
        return attempt.attempt_id

    async def update_attempt_paired(self, attempt: Attempt) -> None:
        """Update an attempt that has successfully paired."""
        await self.update_attempts_paired_batch([attempt])

    async def update_attempt_failed(self, attempt: Attempt) -> None:
        """Update an attempt that has failed (settlement or shutdown)."""
        await self.update_attempts_failed_batch([attempt])

    # ------------------------------------------------------------------
    # Attempts — batch (hot-path: one round-trip per cycle)
    # ------------------------------------------------------------------

    @staticmethod
    def _attempt_insert_params(attempt: Attempt) -> tuple:
        """Build the parameter tuple for an INSERT INTO Attempts."""
        return (
            attempt.market_id, attempt.parameter_set_id,
            attempt.cycle_number,
            attempt.t1_timestamp.isoformat(),
            attempt.first_leg_side.value,
            attempt.P1_points, attempt.reference_yes_points,
            attempt.reference_no_points, attempt.opposite_side.value,
            attempt.opposite_trigger_points, attempt.opposite_max_points,
            attempt.status.value, attempt.time_remaining_at_start,
            attempt.time_remaining_bucket,
            attempt.yes_spread_entry_points,
            attempt.no_spread_entry_points,
            attempt.delta_points,
            attempt.S0_points,
            attempt.stop_loss_threshold_points,
            attempt.stop_loss_price_points,
        )

    @staticmethod
    def _attempt_paired_params(attempt: Attempt) -> tuple:
        """Build the parameter tuple for a paired UPDATE."""
        return (
            attempt.status.value,
            attempt.t2_timestamp.isoformat() if attempt.t2_timestamp else None,
            attempt.t2_cycle_number,
            attempt.time_to_pair_seconds,
            attempt.time_remaining_at_completion,
            attempt.actual_opposite_price,
            attempt.pair_cost_points,
            attempt.pair_profit_points,
            int(attempt.had_feed_gap),
            attempt.closest_approach_points,
            attempt.closest_approach_timestamp.isoformat()
            if attempt.closest_approach_timestamp else None,
            attempt.closest_approach_cycle_number,
            attempt.max_adverse_excursion_points,
            attempt.mae_timestamp.isoformat()
            if attempt.mae_timestamp else None,
            attempt.mae_cycle_number,
            attempt.yes_spread_exit_points,
            attempt.no_spread_exit_points,
            attempt.attempt_id,
        )

    @staticmethod
    def _attempt_failed_params(attempt: Attempt) -> tuple:
        """Build the parameter tuple for a failed UPDATE."""
        return (
            attempt.status.value,
            attempt.time_remaining_at_completion,
            attempt.fail_reason,
            int(attempt.had_feed_gap),
            attempt.closest_approach_points,
            attempt.closest_approach_timestamp.isoformat()
            if attempt.closest_approach_timestamp else None,
            attempt.closest_approach_cycle_number,
            attempt.max_adverse_excursion_points,
            attempt.mae_timestamp.isoformat()
            if attempt.mae_timestamp else None,
            attempt.mae_cycle_number,
            attempt.attempt_id,
        )

    async def insert_attempts_batch(self, attempts: list[Attempt]) -> None:
        """Insert multiple attempts in a single transaction.

        Assigns ``attempt_id`` on each Attempt object.
        Uses one connection + transaction to avoid per-row round-trips.
        """
        if not attempts:
            return

        insert_sql = """INSERT INTO Attempts
                 (market_id, parameter_set_id, cycle_number, t1_timestamp,
                  first_leg_side, P1_points, reference_yes_points,
                  reference_no_points, opposite_side,
                  opposite_trigger_points, opposite_max_points,
                  status, time_remaining_at_start,
                  time_remaining_bucket,
                  yes_spread_entry_points, no_spread_entry_points,
                  delta_points, S0_points,
                  stop_loss_threshold_points, stop_loss_price_points)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        if self._is_postgres:
            pg_sql = _q(insert_sql) + " RETURNING attempt_id"
            async with self._pool.acquire() as conn:
                async with conn.transaction():
                    for attempt in attempts:
                        row = await conn.fetchrow(
                            pg_sql, *self._attempt_insert_params(attempt),
                        )
                        attempt.attempt_id = row["attempt_id"]
        else:
            async with self._write_lock:
                for attempt in attempts:
                    cursor = await self._db.execute(
                        insert_sql, self._attempt_insert_params(attempt),
                    )
                    attempt.attempt_id = cursor.lastrowid
                await self._db.commit()       # single commit for all

    async def update_attempts_paired_batch(self, attempts: list[Attempt]) -> None:
        """Update multiple paired attempts in a single transaction."""
        if not attempts:
            return

        sql = """UPDATE Attempts SET
                 status = ?, t2_timestamp = ?, t2_cycle_number = ?,
                 time_to_pair_seconds = ?, time_remaining_at_completion = ?,
                 actual_opposite_price = ?, pair_cost_points = ?,
                 pair_profit_points = ?, had_feed_gap = ?,
                 closest_approach_points = ?,
                 closest_approach_timestamp = ?,
                 closest_approach_cycle_number = ?,
                 max_adverse_excursion_points = ?,
                 mae_timestamp = ?, mae_cycle_number = ?,
                 yes_spread_exit_points = ?, no_spread_exit_points = ?
                 WHERE attempt_id = ?"""
        params_list = [self._attempt_paired_params(a) for a in attempts]
        await self._executemany(sql, params_list)

    async def update_attempts_failed_batch(self, attempts: list[Attempt]) -> None:
        """Update multiple failed attempts in a single transaction."""
        if not attempts:
            return

        sql = """UPDATE Attempts SET
                 status = ?, time_remaining_at_completion = ?,
                 fail_reason = ?, had_feed_gap = ?,
                 closest_approach_points = ?,
                 closest_approach_timestamp = ?,
                 closest_approach_cycle_number = ?,
                 max_adverse_excursion_points = ?,
                 mae_timestamp = ?, mae_cycle_number = ?
                 WHERE attempt_id = ?"""
        params_list = [self._attempt_failed_params(a) for a in attempts]
        await self._executemany(sql, params_list)

    # ------------------------------------------------------------------
    # Attempts — stop loss (hybrid of paired + failed fields)
    # ------------------------------------------------------------------

    @staticmethod
    def _attempt_stopped_params(attempt: Attempt) -> tuple:
        """Build the parameter tuple for a stop-loss UPDATE.

        Stop-loss exits have fields from both the paired path (t2_timestamp,
        pair_profit_points, exit spreads) and the failed path (fail_reason).
        """
        return (
            attempt.status.value,
            attempt.t2_timestamp.isoformat() if attempt.t2_timestamp else None,
            attempt.t2_cycle_number,
            attempt.time_to_pair_seconds,
            attempt.time_remaining_at_completion,
            attempt.fail_reason,
            attempt.pair_cost_points,
            attempt.pair_profit_points,
            int(attempt.had_feed_gap),
            attempt.closest_approach_points,
            attempt.closest_approach_timestamp.isoformat()
            if attempt.closest_approach_timestamp else None,
            attempt.closest_approach_cycle_number,
            attempt.max_adverse_excursion_points,
            attempt.mae_timestamp.isoformat()
            if attempt.mae_timestamp else None,
            attempt.mae_cycle_number,
            attempt.yes_spread_exit_points,
            attempt.no_spread_exit_points,
            attempt.attempt_id,
        )

    async def update_attempts_stopped_batch(self, attempts: list[Attempt]) -> None:
        """Update multiple stop-loss attempts in a single transaction."""
        if not attempts:
            return

        sql = """UPDATE Attempts SET
                 status = ?, t2_timestamp = ?, t2_cycle_number = ?,
                 time_to_pair_seconds = ?, time_remaining_at_completion = ?,
                 fail_reason = ?, pair_cost_points = ?,
                 pair_profit_points = ?, had_feed_gap = ?,
                 closest_approach_points = ?,
                 closest_approach_timestamp = ?,
                 closest_approach_cycle_number = ?,
                 max_adverse_excursion_points = ?,
                 mae_timestamp = ?, mae_cycle_number = ?,
                 yes_spread_exit_points = ?, no_spread_exit_points = ?
                 WHERE attempt_id = ?"""
        params_list = [self._attempt_stopped_params(a) for a in attempts]
        await self._executemany(sql, params_list)

    # ------------------------------------------------------------------
    # Market summary
    # ------------------------------------------------------------------

    async def update_market_summary(
        self,
        market_id: str,
        total_attempts: int,
        total_pairs: int,
        total_failed: int,
        settlement_failures: int,
        pair_rate: Optional[float],
        avg_time_to_pair: Optional[float],
        median_time_to_pair: Optional[float],
        max_concurrent: int,
        total_cycles: int,
        anomaly_count: int,
        notes: str = "",
    ) -> None:
        """Write final summary statistics to the Markets row."""
        sql = """UPDATE Markets SET
                 total_attempts = ?, total_pairs = ?, total_failed = ?,
                 settlement_failures = ?, pair_rate = ?,
                 avg_time_to_pair = ?, median_time_to_pair = ?,
                 max_concurrent_attempts = ?, total_cycles_run = ?,
                 anomaly_count = ?, actual_settlement_time = ?, notes = ?
                 WHERE market_id = ?"""
        params = (
            total_attempts, total_pairs, total_failed,
            settlement_failures, pair_rate,
            avg_time_to_pair, median_time_to_pair,
            max_concurrent, total_cycles, anomaly_count,
            datetime.now(timezone.utc).isoformat(), notes,
            market_id,
        )
        await self._execute(sql, params)
        logger.debug("Updated market summary for %s", market_id)

    # ------------------------------------------------------------------
    # Snapshots (optional)
    # ------------------------------------------------------------------

    async def insert_snapshot(self, snapshot: Snapshot) -> None:
        """Insert a cycle snapshot (used when enable_snapshots is True)."""
        sql = """INSERT INTO Snapshots
                 (market_id, cycle_number, timestamp, yes_bid_points,
                  yes_ask_points, no_bid_points, no_ask_points,
                  yes_last_trade_points, no_last_trade_points,
                  time_remaining, active_attempts_count, anomaly_flag,
                  yes_period_low_ask_points, no_period_low_ask_points)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        params = (
            snapshot.market_id, snapshot.cycle_number,
            snapshot.timestamp.isoformat(),
            snapshot.yes_bid_points, snapshot.yes_ask_points,
            snapshot.no_bid_points, snapshot.no_ask_points,
            snapshot.yes_last_trade_points, snapshot.no_last_trade_points,
            snapshot.time_remaining_seconds,
            snapshot.active_attempts_count, int(snapshot.anomaly_flag),
            snapshot.yes_period_low_ask_points,
            snapshot.no_period_low_ask_points,
        )
        await self._execute(sql, params)

    # ------------------------------------------------------------------
    # AttemptLifecycle (optional, high-volume)
    # ------------------------------------------------------------------

    async def insert_lifecycle_batch(self, records: list[LifecycleRecord]) -> None:
        """Batch-insert lifecycle tracking records."""
        sql = """INSERT INTO AttemptLifecycle
                 (attempt_id, cycle_number, timestamp,
                  opposite_ask_points, distance_to_trigger,
                  closest_approach_so_far)
                 VALUES (?, ?, ?, ?, ?, ?)"""
        params_list = [
            (
                r.attempt_id, r.cycle_number, r.timestamp.isoformat(),
                r.opposite_ask_points, r.distance_to_trigger,
                r.closest_approach_so_far,
            )
            for r in records
        ]
        await self._executemany(sql, params_list)
