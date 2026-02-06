"""SQLite database schema and async CRUD operations.

Tables: ParameterSets, Markets, Attempts, Snapshots, AttemptLifecycle.
Uses aiosqlite for non-blocking writes from the asyncio event loop.
An asyncio.Lock serialises write operations so multiple MarketMonitor
coroutines can safely share one Database instance.
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiosqlite

from .models import (
    Attempt, AttemptStatus, LifecycleRecord, MarketInfo, ParameterSet,
    Side, Snapshot,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
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
    -- Feature 1: Closest approach to opposite trigger
    closest_approach_points INTEGER,
    closest_approach_timestamp TEXT,
    closest_approach_cycle_number INTEGER,
    -- Feature 2: Max Adverse Excursion on first leg
    max_adverse_excursion_points INTEGER,
    mae_timestamp           TEXT,
    mae_cycle_number        INTEGER,
    -- Feature 3: Time remaining bucket at entry
    time_remaining_bucket   TEXT,
    -- Feature 5: Spread at entry and completion
    yes_spread_entry_points INTEGER,
    no_spread_entry_points  INTEGER,
    yes_spread_exit_points  INTEGER,
    no_spread_exit_points   INTEGER
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
    anomaly_flag            INTEGER DEFAULT 0
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


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    """Async SQLite database manager with a write lock for concurrency."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None
        self._write_lock = asyncio.Lock()

    # --- Lifecycle ---

    async def initialize(self) -> None:
        """Create database file (if needed) and ensure all tables exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self.db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(SCHEMA_SQL)
        await self._db.commit()

        # --- Migrations for existing databases ---
        await self._run_migrations()

        logger.info("Database initialized at %s", self.db_path)

    async def _run_migrations(self) -> None:
        """Add columns / tables that may be missing in older databases."""
        assert self._db is not None
        migrations = [
            "ALTER TABLE Attempts ADD COLUMN closest_approach_points INTEGER",
            "ALTER TABLE Attempts ADD COLUMN closest_approach_timestamp TEXT",
            "ALTER TABLE Attempts ADD COLUMN closest_approach_cycle_number INTEGER",
            "ALTER TABLE Attempts ADD COLUMN max_adverse_excursion_points INTEGER",
            "ALTER TABLE Attempts ADD COLUMN mae_timestamp TEXT",
            "ALTER TABLE Attempts ADD COLUMN mae_cycle_number INTEGER",
            "ALTER TABLE Attempts ADD COLUMN time_remaining_bucket TEXT",
            "ALTER TABLE Attempts ADD COLUMN yes_spread_entry_points INTEGER",
            "ALTER TABLE Attempts ADD COLUMN no_spread_entry_points INTEGER",
            "ALTER TABLE Attempts ADD COLUMN yes_spread_exit_points INTEGER",
            "ALTER TABLE Attempts ADD COLUMN no_spread_exit_points INTEGER",
        ]
        for sql in migrations:
            try:
                await self._db.execute(sql)
                await self._db.commit()
            except Exception:
                pass  # Column already exists

    async def close(self) -> None:
        """Close the database connection."""
        if self._db:
            await self._db.close()
            self._db = None
            logger.info("Database connection closed")

    # --- ParameterSets ---

    async def insert_parameter_set(
        self,
        ps: ParameterSet,
        sampling_mode: str,
        cycle_interval: float,
        cycles_per_market: int,
        feed_gap_threshold: float,
    ) -> int:
        """Insert a parameter set and return its auto-generated ID."""
        assert self._db is not None
        async with self._write_lock:
            cursor = await self._db.execute(
                """INSERT INTO ParameterSets
                   (name, S0_points, delta_points, PairCap_points, trigger_rule,
                    reference_price_source, sampling_mode, cycle_interval_seconds,
                    cycles_per_market, feed_gap_threshold_seconds, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    ps.name, ps.S0_points, ps.delta_points, ps.pair_cap_points,
                    ps.trigger_rule.value, ps.reference_price_source.value,
                    sampling_mode, cycle_interval, cycles_per_market,
                    feed_gap_threshold,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            await self._db.commit()
            ps.parameter_set_id = cursor.lastrowid
        logger.info("Inserted parameter set '%s' with id=%d", ps.name, ps.parameter_set_id)
        return ps.parameter_set_id

    # --- Markets ---

    async def insert_market(
        self,
        market_info: MarketInfo,
        parameter_set_id: int,
        start_time: datetime,
        time_remaining: float,
        cycle_interval: float,
    ) -> None:
        """Insert a new market record."""
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                """INSERT OR REPLACE INTO Markets
                   (market_id, crypto_asset, condition_id, yes_token_id, no_token_id,
                    start_time, settlement_time, tick_size_points, parameter_set_id,
                    time_remaining_at_start, cycle_interval_seconds)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    market_info.market_slug, market_info.crypto_asset,
                    market_info.condition_id, market_info.yes_token_id,
                    market_info.no_token_id, start_time.isoformat(),
                    market_info.settlement_time.isoformat(),
                    market_info.tick_size_points, parameter_set_id,
                    time_remaining, cycle_interval,
                ),
            )
            await self._db.commit()
        logger.debug("Inserted market %s", market_info.market_slug)

    # --- Attempts ---

    async def insert_attempt(self, attempt: Attempt) -> int:
        """Insert a new attempt and return its auto-generated ID."""
        assert self._db is not None
        async with self._write_lock:
            cursor = await self._db.execute(
                """INSERT INTO Attempts
                   (market_id, parameter_set_id, cycle_number, t1_timestamp,
                    first_leg_side, P1_points, reference_yes_points, reference_no_points,
                    opposite_side, opposite_trigger_points, opposite_max_points,
                    status, time_remaining_at_start,
                    time_remaining_bucket,
                    yes_spread_entry_points, no_spread_entry_points)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    attempt.market_id, attempt.parameter_set_id, attempt.cycle_number,
                    attempt.t1_timestamp.isoformat(), attempt.first_leg_side.value,
                    attempt.P1_points, attempt.reference_yes_points,
                    attempt.reference_no_points, attempt.opposite_side.value,
                    attempt.opposite_trigger_points, attempt.opposite_max_points,
                    attempt.status.value, attempt.time_remaining_at_start,
                    attempt.time_remaining_bucket,
                    attempt.yes_spread_entry_points, attempt.no_spread_entry_points,
                ),
            )
            await self._db.commit()
            attempt.attempt_id = cursor.lastrowid
        return attempt.attempt_id

    async def update_attempt_paired(self, attempt: Attempt) -> None:
        """Update an attempt that has successfully paired."""
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                """UPDATE Attempts SET
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
                   WHERE attempt_id = ?""",
                (
                    attempt.status.value,
                    attempt.t2_timestamp.isoformat() if attempt.t2_timestamp else None,
                    attempt.t2_cycle_number, attempt.time_to_pair_seconds,
                    attempt.time_remaining_at_completion, attempt.actual_opposite_price,
                    attempt.pair_cost_points, attempt.pair_profit_points,
                    int(attempt.had_feed_gap),
                    attempt.closest_approach_points,
                    attempt.closest_approach_timestamp.isoformat() if attempt.closest_approach_timestamp else None,
                    attempt.closest_approach_cycle_number,
                    attempt.max_adverse_excursion_points,
                    attempt.mae_timestamp.isoformat() if attempt.mae_timestamp else None,
                    attempt.mae_cycle_number,
                    attempt.yes_spread_exit_points, attempt.no_spread_exit_points,
                    attempt.attempt_id,
                ),
            )
            await self._db.commit()

    async def update_attempt_failed(self, attempt: Attempt) -> None:
        """Update an attempt that has failed (settlement reached or shutdown)."""
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                """UPDATE Attempts SET
                   status = ?, time_remaining_at_completion = ?,
                   fail_reason = ?, had_feed_gap = ?,
                   closest_approach_points = ?,
                   closest_approach_timestamp = ?,
                   closest_approach_cycle_number = ?,
                   max_adverse_excursion_points = ?,
                   mae_timestamp = ?, mae_cycle_number = ?
                   WHERE attempt_id = ?""",
                (
                    attempt.status.value, attempt.time_remaining_at_completion,
                    attempt.fail_reason, int(attempt.had_feed_gap),
                    attempt.closest_approach_points,
                    attempt.closest_approach_timestamp.isoformat() if attempt.closest_approach_timestamp else None,
                    attempt.closest_approach_cycle_number,
                    attempt.max_adverse_excursion_points,
                    attempt.mae_timestamp.isoformat() if attempt.mae_timestamp else None,
                    attempt.mae_cycle_number,
                    attempt.attempt_id,
                ),
            )
            await self._db.commit()

    # --- Market summary ---

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
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                """UPDATE Markets SET
                   total_attempts = ?, total_pairs = ?, total_failed = ?,
                   settlement_failures = ?, pair_rate = ?,
                   avg_time_to_pair = ?, median_time_to_pair = ?,
                   max_concurrent_attempts = ?, total_cycles_run = ?,
                   anomaly_count = ?, actual_settlement_time = ?, notes = ?
                   WHERE market_id = ?""",
                (
                    total_attempts, total_pairs, total_failed, settlement_failures,
                    pair_rate, avg_time_to_pair, median_time_to_pair,
                    max_concurrent, total_cycles, anomaly_count,
                    datetime.now(timezone.utc).isoformat(), notes, market_id,
                ),
            )
            await self._db.commit()
        logger.debug("Updated market summary for %s", market_id)

    # --- Snapshots (optional) ---

    async def insert_snapshot(self, snapshot: Snapshot) -> None:
        """Insert a cycle snapshot (used when enable_snapshots is True)."""
        assert self._db is not None
        async with self._write_lock:
            await self._db.execute(
                """INSERT INTO Snapshots
                   (market_id, cycle_number, timestamp, yes_bid_points, yes_ask_points,
                    no_bid_points, no_ask_points, yes_last_trade_points,
                    no_last_trade_points, time_remaining, active_attempts_count,
                    anomaly_flag)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    snapshot.market_id, snapshot.cycle_number,
                    snapshot.timestamp.isoformat(), snapshot.yes_bid_points,
                    snapshot.yes_ask_points, snapshot.no_bid_points,
                    snapshot.no_ask_points, snapshot.yes_last_trade_points,
                    snapshot.no_last_trade_points, snapshot.time_remaining_seconds,
                    snapshot.active_attempts_count, int(snapshot.anomaly_flag),
                ),
            )
            await self._db.commit()

    # --- AttemptLifecycle (optional, high-volume) ---

    async def insert_lifecycle_batch(self, records: list[LifecycleRecord]) -> None:
        """Batch-insert lifecycle tracking records."""
        if not records:
            return
        assert self._db is not None
        async with self._write_lock:
            await self._db.executemany(
                """INSERT INTO AttemptLifecycle
                   (attempt_id, cycle_number, timestamp,
                    opposite_ask_points, distance_to_trigger, closest_approach_so_far)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                [
                    (r.attempt_id, r.cycle_number, r.timestamp.isoformat(),
                     r.opposite_ask_points, r.distance_to_trigger,
                     r.closest_approach_so_far)
                    for r in records
                ],
            )
            await self._db.commit()
