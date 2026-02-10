"""Analysis query functions for post-run reporting.

Supports both PostgreSQL (``asyncpg``) and SQLite (``aiosqlite``) backends.
When *db_source* looks like a ``postgres://`` URL the PostgreSQL adapter is
used; otherwise it is treated as a local SQLite file path.

Each function opens its own connection, runs an aggregate query, and
returns the result as plain Python dicts / lists.  All functions accept
optional *parameter_set_id* and *crypto_asset* filters.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Dual-backend helpers
# ---------------------------------------------------------------------------

def _is_pg(source: str) -> bool:
    """Return True when *source* looks like a PostgreSQL connection URL."""
    return "postgres" in source.lower()


def _q(sql: str) -> str:
    """Convert ``?`` placeholders to ``$1, $2, …`` for PostgreSQL."""
    parts = sql.split("?")
    if len(parts) <= 1:
        return sql
    result = parts[0]
    for i, part in enumerate(parts[1:], 1):
        result += f"${i}" + part
    return result


class _PgAdapter:
    """Thin wrapper around an ``asyncpg.Connection``."""

    def __init__(self, conn):
        self._conn = conn

    async def fetch_all(self, sql: str, params: list | None = None) -> list[dict]:
        params = params or []
        rows = await self._conn.fetch(_q(sql), *params)
        return [dict(r) for r in rows]

    async def fetch_one(self, sql: str, params: list | None = None) -> dict:
        params = params or []
        row = await self._conn.fetchrow(_q(sql), *params)
        return dict(row) if row else {}


class _SqliteAdapter:
    """Thin wrapper around an ``aiosqlite.Connection``."""

    def __init__(self, db):
        self._db = db

    async def fetch_all(self, sql: str, params: list | None = None) -> list[dict]:
        params = params or []
        async with self._db.execute(sql, params) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def fetch_one(self, sql: str, params: list | None = None) -> dict:
        params = params or []
        async with self._db.execute(sql, params) as cur:
            row = await cur.fetchone()
            return dict(row) if row else {}


@asynccontextmanager
async def _connect(db_source: str):
    """Yield a backend-agnostic adapter for *db_source*.

    For PostgreSQL connections, retries up to 3 times with exponential
    back-off to handle transient authentication / pooler failures.
    """
    if _is_pg(db_source):
        import asyncpg
        last_exc: BaseException | None = None
        for attempt in range(3):
            try:
                conn = await asyncpg.connect(db_source, statement_cache_size=0)
                break
            except (asyncpg.ConnectionFailureError, OSError) as exc:
                last_exc = exc
                wait = 1.0 * (2 ** attempt)
                logger.warning("PG connect attempt %d failed (%s), retrying in %.1fs …",
                               attempt + 1, exc, wait)
                await asyncio.sleep(wait)
        else:
            raise last_exc  # type: ignore[misc]
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


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------

def _where(
    parameter_set_id: Optional[int] = None,
    crypto_asset: Optional[str] = None,
    date_after: Optional[str] = None,
    table_prefix: str = "a",
) -> tuple[str, list]:
    """Build a WHERE clause + params from optional filters."""
    clauses: list[str] = []
    params: list = []
    if parameter_set_id is not None:
        clauses.append(f"{table_prefix}.parameter_set_id = ?")
        params.append(parameter_set_id)
    if crypto_asset is not None:
        clauses.append("m.crypto_asset = ?")
        params.append(crypto_asset.lower())
    if date_after is not None:
        clauses.append(f"{table_prefix}.t1_timestamp >= ?")
        params.append(date_after)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def _safe_div(a, b, default=0.0):
    return a / b if b else default


# ---------------------------------------------------------------------------
# Core queries
# ---------------------------------------------------------------------------

async def get_overall_stats(
    db_source: str,
    parameter_set_id: Optional[int] = None,
    crypto_asset: Optional[str] = None,
    date_after: Optional[str] = None,
) -> dict:
    """Total attempts, pairs, pair_rate, avg/median time_to_pair."""
    where, params = _where(parameter_set_id, crypto_asset, date_after)
    join = "JOIN Markets m ON a.market_id = m.market_id" if crypto_asset else ""

    sql = f"""
        SELECT
            COUNT(*) as total_attempts,
            SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END) as total_pairs,
            SUM(CASE WHEN a.status='completed_failed' THEN 1 ELSE 0 END) as total_failed,
            AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
            AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp,
            AVG(CASE WHEN a.status='completed_paired' THEN a.pair_cost_points END) as avg_cost,
            AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END) as avg_profit
        FROM Attempts a {join} {where}
    """
    async with _connect(db_source) as db:
        return await db.fetch_one(sql, params)


async def get_stats_by_asset(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Breakdown by crypto_asset."""
    ps_clause = "WHERE a.parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT m.crypto_asset,
               COUNT(*) as attempts,
               SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END) as pairs,
               AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
               AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp
        FROM Attempts a
        JOIN Markets m ON a.market_id = m.market_id
        {ps_clause}
        GROUP BY m.crypto_asset
        ORDER BY m.crypto_asset
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_time_to_pair_distribution(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Histogram buckets for time-to-pair."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT
          CASE
            WHEN time_to_pair_seconds < 10 THEN '0-10s'
            WHEN time_to_pair_seconds < 30 THEN '10-30s'
            WHEN time_to_pair_seconds < 60 THEN '30-60s'
            WHEN time_to_pair_seconds < 120 THEN '60-120s'
            WHEN time_to_pair_seconds < 300 THEN '120-300s'
            ELSE '300s+'
          END as bucket,
          COUNT(*) as count,
          AVG(pair_profit_points) as avg_profit
        FROM Attempts
        WHERE status = 'completed_paired' {ps_clause}
        GROUP BY bucket
        ORDER BY MIN(time_to_pair_seconds)
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_stats_by_first_leg(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """YES-first vs NO-first breakdown with MAE and profit."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT first_leg_side,
               COUNT(*) as attempts,
               SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
               AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
               AVG(CASE WHEN status='completed_paired' THEN time_to_pair_seconds END) as avg_ttp,
               AVG(CASE WHEN status='completed_paired' THEN pair_profit_points END) as avg_profit,
               AVG(max_adverse_excursion_points) as avg_mae
        FROM Attempts {ps_clause}
        GROUP BY first_leg_side
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_stats_by_market_phase(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Early (10min+), Middle (5-10min), Late (0-5min)."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT
          CASE
            WHEN time_remaining_at_start > 600 THEN 'Early (10min+)'
            WHEN time_remaining_at_start > 300 THEN 'Middle (5-10min)'
            ELSE 'Late (0-5min)'
          END as phase,
          COUNT(*) as attempts,
          SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
          AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
        FROM Attempts {ps_clause}
        GROUP BY phase
        ORDER BY MIN(time_remaining_at_start) DESC
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_stats_by_reference_regime(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Balanced (45-55), YES-favored (56-70), NO-favored (30-44), Extreme."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT
          CASE
            WHEN reference_yes_points BETWEEN 45 AND 55 THEN 'Balanced (45-55)'
            WHEN reference_yes_points BETWEEN 56 AND 70 THEN 'YES-favored (56-70)'
            WHEN reference_yes_points BETWEEN 30 AND 44 THEN 'NO-favored (30-44)'
            ELSE 'Extreme (<30 or >70)'
          END as regime,
          COUNT(*) as attempts,
          SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
          AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
        FROM Attempts {ps_clause}
        GROUP BY regime
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_stats_by_time_bucket(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Pair rate by minute remaining at entry (15 min down to 0 min)."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT
          CASE
            WHEN time_remaining_at_start >= 840 THEN '15 min'
            WHEN time_remaining_at_start >= 780 THEN '14 min'
            WHEN time_remaining_at_start >= 720 THEN '13 min'
            WHEN time_remaining_at_start >= 660 THEN '12 min'
            WHEN time_remaining_at_start >= 600 THEN '11 min'
            WHEN time_remaining_at_start >= 540 THEN '10 min'
            WHEN time_remaining_at_start >= 480 THEN '9 min'
            WHEN time_remaining_at_start >= 420 THEN '8 min'
            WHEN time_remaining_at_start >= 360 THEN '7 min'
            WHEN time_remaining_at_start >= 300 THEN '6 min'
            WHEN time_remaining_at_start >= 240 THEN '5 min'
            WHEN time_remaining_at_start >= 180 THEN '4 min'
            WHEN time_remaining_at_start >= 120 THEN '3 min'
            WHEN time_remaining_at_start >= 60 THEN '2 min'
            WHEN time_remaining_at_start >= 0 THEN '1 min'
            ELSE '0 min'
          END as bucket,
          COUNT(*) as attempts,
          SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
          AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
          AVG(CASE WHEN status='completed_paired' THEN time_to_pair_seconds END) as avg_ttp,
          AVG(CASE WHEN status='completed_paired' THEN pair_profit_points END) as avg_profit,
          AVG(max_adverse_excursion_points) as avg_mae
        FROM Attempts {ps_clause}
        GROUP BY bucket
        ORDER BY MIN(time_remaining_at_start) DESC
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_mae_analysis(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Max Adverse Excursion distribution for risk profiling."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with _connect(db_source) as db:
        # Overall MAE stats
        sql_overall = f"""
            SELECT
                COUNT(*) as total,
                AVG(max_adverse_excursion_points) as avg_mae,
                MAX(max_adverse_excursion_points) as max_mae,
                MIN(max_adverse_excursion_points) as min_mae
            FROM Attempts
            WHERE max_adverse_excursion_points IS NOT NULL {ps_clause}
        """
        overall = await db.fetch_one(sql_overall, ps_params)

        # MAE by outcome (paired vs failed)
        sql_by_outcome = f"""
            SELECT
                status,
                COUNT(*) as count,
                AVG(max_adverse_excursion_points) as avg_mae,
                MAX(max_adverse_excursion_points) as max_mae
            FROM Attempts
            WHERE max_adverse_excursion_points IS NOT NULL {ps_clause}
            GROUP BY status
        """
        by_outcome = await db.fetch_all(sql_by_outcome, ps_params)

        # MAE bucket distribution
        sql_buckets = f"""
            SELECT
              CASE
                WHEN max_adverse_excursion_points = 0 THEN '0 (no loss)'
                WHEN max_adverse_excursion_points <= 2 THEN '1-2 pts'
                WHEN max_adverse_excursion_points <= 5 THEN '3-5 pts'
                WHEN max_adverse_excursion_points <= 10 THEN '6-10 pts'
                ELSE '10+ pts'
              END as bucket,
              COUNT(*) as count,
              AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
            FROM Attempts
            WHERE max_adverse_excursion_points IS NOT NULL {ps_clause}
            GROUP BY bucket
            ORDER BY MIN(max_adverse_excursion_points)
        """
        buckets = await db.fetch_all(sql_buckets, ps_params)

    return {"overall": overall, "by_outcome": by_outcome, "buckets": buckets}


async def get_spread_analysis(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Spread at entry and exit analysis."""
    ps_clause_where = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_clause_and = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with _connect(db_source) as db:
        # Entry spreads (all attempts)
        sql_entry = f"""
            SELECT
                AVG(yes_spread_entry_points) as avg_yes_spread_entry,
                AVG(no_spread_entry_points) as avg_no_spread_entry,
                MAX(yes_spread_entry_points) as max_yes_spread_entry,
                MAX(no_spread_entry_points) as max_no_spread_entry,
                MIN(yes_spread_entry_points) as min_yes_spread_entry,
                MIN(no_spread_entry_points) as min_no_spread_entry
            FROM Attempts
            {ps_clause_where}
        """
        entry = await db.fetch_one(sql_entry, ps_params)

        # Exit spreads (paired only)
        sql_exit = f"""
            SELECT
                AVG(yes_spread_exit_points) as avg_yes_spread_exit,
                AVG(no_spread_exit_points) as avg_no_spread_exit,
                MAX(yes_spread_exit_points) as max_yes_spread_exit,
                MAX(no_spread_exit_points) as max_no_spread_exit
            FROM Attempts
            WHERE status = 'completed_paired'
              AND yes_spread_exit_points IS NOT NULL
              {ps_clause_and}
        """
        exit_data = await db.fetch_one(sql_exit, ps_params)

        # Spread vs pair rate
        sql_spread_rate = f"""
            SELECT
              CASE
                WHEN (yes_spread_entry_points + no_spread_entry_points) <= 2 THEN 'Tight (<=2)'
                WHEN (yes_spread_entry_points + no_spread_entry_points) <= 4 THEN 'Normal (3-4)'
                WHEN (yes_spread_entry_points + no_spread_entry_points) <= 6 THEN 'Wide (5-6)'
                ELSE 'Very wide (7+)'
              END as combined_spread_bucket,
              COUNT(*) as attempts,
              SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
              AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
              AVG(CASE WHEN status='completed_paired' THEN time_to_pair_seconds END) as avg_ttp
            FROM Attempts
            WHERE yes_spread_entry_points IS NOT NULL
              AND no_spread_entry_points IS NOT NULL
              {ps_clause_and}
            GROUP BY combined_spread_bucket
            ORDER BY MIN(yes_spread_entry_points + no_spread_entry_points)
        """
        by_spread = await db.fetch_all(sql_spread_rate, ps_params)

    return {"entry": entry, "exit": exit_data, "by_combined_spread": by_spread}


async def get_stats_by_market_minute(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Pair rate by position within the 15-min window (5 x 3-min buckets)."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT
          CASE
            WHEN time_remaining_at_start > 720 THEN '00-03 min'
            WHEN time_remaining_at_start > 540 THEN '03-06 min'
            WHEN time_remaining_at_start > 360 THEN '06-09 min'
            WHEN time_remaining_at_start > 180 THEN '09-12 min'
            ELSE                                    '12-15 min'
          END as bucket,
          COUNT(*) as attempts,
          SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
          AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
          AVG(CASE WHEN status='completed_paired' THEN time_to_pair_seconds END) as avg_ttp,
          AVG(CASE WHEN status='completed_paired' THEN pair_profit_points END) as avg_profit
        FROM Attempts {ps_clause}
        GROUP BY bucket
        ORDER BY MIN(time_remaining_at_start) DESC
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_cross_market_consistency(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Per-market pair_rate, sorted for variance/consistency analysis."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT market_id,
               COUNT(*) as attempts,
               SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) as pairs,
               AVG(CASE WHEN status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
        FROM Attempts {ps_clause}
        GROUP BY market_id
        HAVING COUNT(*) >= 2
        ORDER BY pair_rate DESC
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_pair_cost_distribution(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Cheap (<90), Medium (90-95), Expensive (>95)."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    sql = f"""
        SELECT
          CASE
            WHEN pair_cost_points < 90 THEN 'Cheap (<90)'
            WHEN pair_cost_points <= 95 THEN 'Medium (90-95)'
            ELSE 'Expensive (>95)'
          END as bucket,
          COUNT(*) as count,
          AVG(pair_profit_points) as avg_profit,
          AVG(time_to_pair_seconds) as avg_ttp
        FROM Attempts
        WHERE status = 'completed_paired' {ps_clause}
        GROUP BY bucket
        ORDER BY MIN(pair_cost_points)
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql, ps_params)


async def get_failure_analysis(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Failed attempts: count by fail_reason, avg time active."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with _connect(db_source) as db:
        sql = f"""
            SELECT
                fail_reason,
                COUNT(*) as count,
                AVG(closest_approach_points) as avg_closest_approach
            FROM Attempts
            WHERE status = 'completed_failed' {ps_clause}
            GROUP BY fail_reason
        """
        by_reason = await db.fetch_all(sql, ps_params)

        sql2 = f"""
            SELECT COUNT(*) as total_failed,
                   AVG(closest_approach_points) as avg_closest
            FROM Attempts
            WHERE status = 'completed_failed' {ps_clause}
        """
        totals = await db.fetch_one(sql2, ps_params)

    return {"by_reason": by_reason, **totals}


async def get_profitability_projection(
    db_source: str,
    parameter_set_id: Optional[int] = None,
    exit_loss_points: int = 2,
    num_assets: int = 4,
) -> dict:
    """Calculate breakeven pair rate, EV per attempt, daily/monthly projection.

    Uses formulas from PROJECT_SPEC §13.4:
      breakeven = L / (profit_avg + L)
      EV = R × profit_avg - (1 - R) × L
    """
    stats = await get_overall_stats(db_source, parameter_set_id)
    total_att = stats.get("total_attempts", 0) or 0
    total_pairs = stats.get("total_pairs", 0) or 0
    avg_profit = stats.get("avg_profit") or 0

    R = _safe_div(total_pairs, total_att)
    L = exit_loss_points
    avg_profit_float = float(avg_profit) if avg_profit else 0.0

    breakeven = _safe_div(L, avg_profit_float + L) if (avg_profit_float + L) > 0 else 1.0
    ev_per_attempt = R * avg_profit_float - (1 - R) * L if total_att else 0

    # Markets per day: each asset has 4 markets/hour × 24h = 96
    markets_per_day = num_assets * 96

    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with _connect(db_source) as db:
        row = await db.fetch_one(
            f"SELECT COUNT(DISTINCT market_id) as n FROM Attempts {ps_clause}",
            ps_params,
        )
        num_markets = row.get("n", 1) or 1

    avg_att_per_market = _safe_div(total_att, max(1, num_markets))
    attempts_per_day = markets_per_day * avg_att_per_market
    daily_ev = attempts_per_day * ev_per_attempt
    monthly_ev = daily_ev * 30

    return {
        "pair_rate": R,
        "avg_profit_points": avg_profit,
        "exit_loss_points": L,
        "breakeven_pair_rate": breakeven,
        "ev_per_attempt": ev_per_attempt,
        "avg_attempts_per_market": avg_att_per_market,
        "markets_per_day": markets_per_day,
        "attempts_per_day": attempts_per_day,
        "daily_ev_points": daily_ev,
        "monthly_ev_points": monthly_ev,
        "daily_ev_dollars": daily_ev / 100,
        "monthly_ev_dollars": monthly_ev / 100,
    }


async def get_parameter_comparison(db_source: str) -> list[dict]:
    """Compare parameter sets grouped by delta and S0."""
    sql = """
        SELECT
            p.S0_points  AS "S0_points",
            p.delta_points,
            COUNT(a.attempt_id) as attempts,
            SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END) as pairs,
            AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
            AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp,
            AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END) as avg_profit
        FROM ParameterSets p
        LEFT JOIN Attempts a ON p.parameter_set_id = a.parameter_set_id
        GROUP BY p.S0_points, p.delta_points
        ORDER BY p.delta_points ASC
    """
    async with _connect(db_source) as db:
        return await db.fetch_all(sql)


async def get_near_miss_analysis(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """For failed attempts: distribution of closest approach to trigger."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with _connect(db_source) as db:
        sql = f"""
            SELECT
              CASE
                WHEN closest_approach_points <= 1 THEN 'Within 1pt'
                WHEN closest_approach_points <= 2 THEN 'Within 2pt'
                WHEN closest_approach_points <= 5 THEN 'Within 5pt'
                WHEN closest_approach_points <= 10 THEN 'Within 10pt'
                ELSE '10pt+'
              END as proximity,
              COUNT(*) as count
            FROM Attempts
            WHERE status = 'completed_failed'
              AND closest_approach_points IS NOT NULL
              {ps_clause}
            GROUP BY proximity
            ORDER BY MIN(closest_approach_points)
        """
        buckets = await db.fetch_all(sql, ps_params)

        # Frustration rate: % within 2 points
        sql2 = f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN closest_approach_points <= 2 THEN 1 ELSE 0 END) as near_misses,
                AVG(closest_approach_points) as avg_closest
            FROM Attempts
            WHERE status = 'completed_failed'
              AND closest_approach_points IS NOT NULL
              {ps_clause}
        """
        totals = await db.fetch_one(sql2, ps_params)

    frustration_rate = _safe_div(
        totals.get("near_misses", 0), totals.get("total", 0)
    )

    return {
        "proximity_buckets": buckets,
        "frustration_rate": frustration_rate,
        **totals,
    }


# ---------------------------------------------------------------------------
# First-leg limit order analytics
# ---------------------------------------------------------------------------

async def get_first_leg_fill_stats(
    db_source: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Statistics on first-leg limit order fills: cycles to fill and taker risk.

    Returns:
        avg_cycles_to_fill: Average cycles the first-leg limit waited.
        same_cycle_pct: % of fills that were same-cycle (highest taker risk).
        avg_placement_buffer: Average distance below ask at placement.
        taker_risk_breakdown: List of dicts with fill_speed x buffer distribution.
    """
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with _connect(db_source) as db:
        # Overall fill stats
        sql = f"""
            SELECT
                AVG(cycles_to_fill_first_leg) as avg_cycles_to_fill,
                AVG(CASE WHEN cycles_to_fill_first_leg = 0 THEN 1.0 ELSE 0.0 END)
                    as same_cycle_pct,
                AVG(placement_buffer_points) as avg_placement_buffer,
                COUNT(*) as total_with_data
            FROM Attempts
            WHERE cycles_to_fill_first_leg IS NOT NULL
              {ps_clause}
        """
        summary = await db.fetch_one(sql, ps_params)

        # Taker risk breakdown: fill speed x buffer
        sql2 = f"""
            SELECT
                CASE WHEN cycles_to_fill_first_leg = 0
                     THEN 'same_cycle'
                     WHEN cycles_to_fill_first_leg = 1
                     THEN '1_cycle'
                     ELSE '2+_cycles'
                END as fill_speed,
                placement_buffer_points as buffer,
                COUNT(*) as fills
            FROM Attempts
            WHERE cycles_to_fill_first_leg IS NOT NULL
              AND placement_buffer_points IS NOT NULL
              {ps_clause}
            GROUP BY fill_speed, buffer
            ORDER BY cycles_to_fill_first_leg, placement_buffer_points
        """
        breakdown = await db.fetch_all(sql2, ps_params)

        # Paired-only: same breakdown but only for successfully paired attempts
        sql3 = f"""
            SELECT
                CASE WHEN cycles_to_fill_first_leg = 0
                     THEN 'same_cycle'
                     WHEN cycles_to_fill_first_leg = 1
                     THEN '1_cycle'
                     ELSE '2+_cycles'
                END as fill_speed,
                COUNT(*) as paired_fills,
                AVG(pair_profit_points) as avg_profit
            FROM Attempts
            WHERE status = 'completed_paired'
              AND cycles_to_fill_first_leg IS NOT NULL
              {ps_clause}
            GROUP BY fill_speed
            ORDER BY cycles_to_fill_first_leg
        """
        paired_by_speed = await db.fetch_all(sql3, ps_params)

    return {
        **summary,
        "taker_risk_breakdown": breakdown,
        "paired_by_fill_speed": paired_by_speed,
    }
