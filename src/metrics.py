"""Analysis query functions for post-run reporting.

Each function opens its own ``aiosqlite`` connection, runs an aggregate
query, and returns the result as plain Python dicts / lists.  All
functions accept optional *parameter_set_id* and *crypto_asset* filters.
"""

from __future__ import annotations

import logging
from typing import Optional

import aiosqlite

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
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
        clauses.append(f"m.crypto_asset = ?")
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
    db_path: str,
    parameter_set_id: Optional[int] = None,
    crypto_asset: Optional[str] = None,
    date_after: Optional[str] = None,
) -> dict:
    """Total attempts, pairs, pair_rate, avg/median time_to_pair."""
    where, params = _where(parameter_set_id, crypto_asset, date_after)
    join = "JOIN Markets m ON a.market_id = m.market_id" if crypto_asset else ""

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, params) as cur:
            row = await cur.fetchone()
            return dict(row) if row else {}


async def get_stats_by_asset(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Breakdown by crypto_asset."""
    ps_clause = "WHERE a.parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_time_to_pair_distribution(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Histogram buckets for time-to-pair."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_stats_by_first_leg(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """YES-first vs NO-first breakdown with MAE and profit."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_stats_by_market_phase(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Early (10min+), Middle (5-10min), Late (0-5min)."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_stats_by_reference_regime(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Balanced (45-55), YES-favored (56-70), NO-favored (30-44), Extreme."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_stats_by_time_bucket(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Pair rate by time_remaining_bucket at entry."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        sql = f"""
            SELECT
              COALESCE(time_remaining_bucket, 'unknown') as bucket,
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_mae_analysis(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Max Adverse Excursion distribution for risk profiling."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

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
        async with db.execute(sql_overall, ps_params) as cur:
            overall = dict(await cur.fetchone())

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
        async with db.execute(sql_by_outcome, ps_params) as cur:
            by_outcome = [dict(r) for r in await cur.fetchall()]

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
        async with db.execute(sql_buckets, ps_params) as cur:
            buckets = [dict(r) for r in await cur.fetchall()]

        return {"overall": overall, "by_outcome": by_outcome, "buckets": buckets}


async def get_spread_analysis(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Spread at entry and exit analysis."""
    ps_clause_where = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_clause_and = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row

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
        async with db.execute(sql_entry, ps_params) as cur:
            entry = dict(await cur.fetchone())

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
        async with db.execute(sql_exit, ps_params) as cur:
            exit_data = dict(await cur.fetchone())

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
        async with db.execute(sql_spread_rate, ps_params) as cur:
            by_spread = [dict(r) for r in await cur.fetchall()]

        return {"entry": entry, "exit": exit_data, "by_combined_spread": by_spread}


async def get_stats_by_market_minute(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Pair rate by position within the 15-min window (5 x 3-min buckets).

    Uses ``time_remaining_at_start`` to place each attempt into a bucket
    representing which 3-minute segment of the market it was triggered in.
    """
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_cross_market_consistency(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Per-market pair_rate, sorted for variance/consistency analysis."""
    ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_pair_cost_distribution(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> list[dict]:
    """Cheap (<90), Medium (90-95), Expensive (>95)."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_failure_analysis(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """Failed attempts: count by fail_reason, avg time active."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        sql = f"""
            SELECT
                fail_reason,
                COUNT(*) as count,
                AVG(closest_approach_points) as avg_closest_approach
            FROM Attempts
            WHERE status = 'completed_failed' {ps_clause}
            GROUP BY fail_reason
        """
        async with db.execute(sql, ps_params) as cur:
            by_reason = [dict(r) for r in await cur.fetchall()]

        sql2 = f"""
            SELECT COUNT(*) as total_failed,
                   AVG(closest_approach_points) as avg_closest
            FROM Attempts
            WHERE status = 'completed_failed' {ps_clause}
        """
        async with db.execute(sql2, ps_params) as cur:
            totals = dict(await cur.fetchone())

        return {"by_reason": by_reason, **totals}


async def get_profitability_projection(
    db_path: str,
    parameter_set_id: Optional[int] = None,
    exit_loss_points: int = 2,
    num_assets: int = 4,
) -> dict:
    """Calculate breakeven pair rate, EV per attempt, daily/monthly projection.

    Uses formulas from PROJECT_SPEC §13.4:
      breakeven = L / (profit_avg + L)
      EV = R × profit_avg - (1 - R) × L
    """
    stats = await get_overall_stats(db_path, parameter_set_id)
    total_att = stats.get("total_attempts", 0) or 0
    total_pairs = stats.get("total_pairs", 0) or 0
    avg_profit = stats.get("avg_profit") or 0

    R = _safe_div(total_pairs, total_att)
    L = exit_loss_points

    breakeven = _safe_div(L, avg_profit + L) if (avg_profit + L) > 0 else 1.0
    ev_per_attempt = R * avg_profit - (1 - R) * L if total_att else 0

    # Markets per day: each asset has 4 markets/hour × 24h = 96
    markets_per_day = num_assets * 96
    # Need to know avg attempts per market
    async with aiosqlite.connect(db_path) as db:
        ps_clause = "WHERE parameter_set_id = ?" if parameter_set_id else ""
        ps_params = [parameter_set_id] if parameter_set_id else []
        async with db.execute(
            f"SELECT COUNT(DISTINCT market_id) as n FROM Attempts {ps_clause}",
            ps_params,
        ) as cur:
            row = await cur.fetchone()
            num_markets = row[0] if row else 1

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


async def get_parameter_comparison(db_path: str) -> list[dict]:
    """Compare all parameter sets side-by-side."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT
                p.parameter_set_id,
                p.name,
                p.S0_points,
                p.delta_points,
                COUNT(a.attempt_id) as attempts,
                SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END) as pairs,
                AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
                AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp,
                AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END) as avg_profit
            FROM ParameterSets p
            LEFT JOIN Attempts a ON p.parameter_set_id = a.parameter_set_id
            GROUP BY p.parameter_set_id
            ORDER BY pair_rate DESC
        """
        async with db.execute(sql) as cur:
            return [dict(r) for r in await cur.fetchall()]


async def get_near_miss_analysis(
    db_path: str,
    parameter_set_id: Optional[int] = None,
) -> dict:
    """For failed attempts: distribution of closest approach to trigger."""
    ps_clause = "AND parameter_set_id = ?" if parameter_set_id else ""
    ps_params = [parameter_set_id] if parameter_set_id else []

    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
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
        async with db.execute(sql, ps_params) as cur:
            buckets = [dict(r) for r in await cur.fetchall()]

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
        async with db.execute(sql2, ps_params) as cur:
            row = await cur.fetchone()
            totals = dict(row) if row else {}

        frustration_rate = _safe_div(
            totals.get("near_misses", 0), totals.get("total", 0)
        )

        return {
            "proximity_buckets": buckets,
            "frustration_rate": frustration_rate,
            **totals,
        }
