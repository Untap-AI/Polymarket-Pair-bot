#!/usr/bin/env python3
"""Break down a config's performance by individual P1 price point.

Usage:
    python scripts/p1_breakdown.py --delta 8 --sl 37 --p1-lo 2 --p1-hi 26 \
        --time-lo 14 --time-hi 15 --fraction 0.10 --markets BTC
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import load_env_file  # noqa: E402
from scripts.optimize_params import (  # noqa: E402
    _query,
    _TAKER_FEE_SQL,
    simulate_compound_bankroll,
    bootstrap_bankroll_stats,
)


def _resolve_db_url(args) -> str:
    if args.db_url:
        return args.db_url
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    print("ERROR: No database URL. Set DATABASE_URL or pass --db-url.", file=sys.stderr)
    sys.exit(1)


async def run(args):
    db_url = _resolve_db_url(args)
    delta = args.delta
    sl = args.sl
    p1_lo = args.p1_lo
    p1_hi = args.p1_hi
    time_lo = args.time_lo
    time_hi = args.time_hi
    fraction = args.fraction
    pair_cap = 100 - delta

    markets_filter = ""
    params: list = [delta, sl, p1_lo, p1_hi, time_lo, time_hi]
    idx = 7
    if args.markets:
        assets = [a.strip().lower() for a in args.markets.split(",")]
        markets_filter = f"AND market_id IN (SELECT market_id FROM Markets WHERE crypto_asset = ANY(${idx}))"
        params.append(assets)

    sql = f"""
        SELECT
            P1_points,
            COUNT(*) AS attempts,
            SUM(CASE WHEN status = 'completed_paired' THEN 1 ELSE 0 END) AS pairs,
            AVG(CASE WHEN status = 'completed_paired' THEN 1.0 ELSE 0.0 END) AS pair_rate,
            AVG(
                CASE
                    WHEN status = 'completed_paired' THEN delta_points
                    WHEN status = 'completed_failed'
                         AND stop_loss_threshold_points IS NOT NULL
                         AND P1_points >= stop_loss_threshold_points
                        THEN -(stop_loss_threshold_points + {_TAKER_FEE_SQL})
                    WHEN status = 'completed_failed' THEN -P1_points
                    ELSE 0
                END
            ) AS avg_pnl,
            SUM(
                CASE
                    WHEN status = 'completed_paired' THEN delta_points
                    WHEN status = 'completed_failed'
                         AND stop_loss_threshold_points IS NOT NULL
                         AND P1_points >= stop_loss_threshold_points
                        THEN -(stop_loss_threshold_points + {_TAKER_FEE_SQL})
                    WHEN status = 'completed_failed' THEN -P1_points
                    ELSE 0
                END
            ) AS total_pnl
        FROM Attempts
        WHERE status IN ('completed_paired', 'completed_failed')
          AND S0_points = 1
          AND delta_points = $1
          AND stop_loss_threshold_points = $2
          AND P1_points BETWEEN $3 AND $4
          AND CEIL(time_remaining_at_start / 60)::int BETWEEN $5 AND $6
          {markets_filter}
        GROUP BY P1_points
        ORDER BY P1_points
    """

    rows = await _query(db_url, sql, params)

    # Per-P1 market outcomes for bankroll sim
    outcomes_sql = f"""
        SELECT DISTINCT ON (market_id, P1_points)
            market_id,
            t1_timestamp,
            status,
            P1_points,
            delta_points,
            CASE
                WHEN stop_loss_threshold_points IS NOT NULL
                     AND P1_points >= stop_loss_threshold_points
                THEN stop_loss_threshold_points
                ELSE P1_points
            END AS loss_points,
            CASE
                WHEN status = 'completed_failed'
                     AND stop_loss_threshold_points IS NOT NULL
                     AND P1_points >= stop_loss_threshold_points
                THEN {_TAKER_FEE_SQL}
                ELSE 0
            END AS taker_fee_points
        FROM Attempts
        WHERE status IN ('completed_paired', 'completed_failed')
          AND S0_points = 1
          AND delta_points = $1
          AND stop_loss_threshold_points = $2
          AND P1_points BETWEEN $3 AND $4
          AND CEIL(time_remaining_at_start / 60)::int BETWEEN $5 AND $6
          {markets_filter}
        ORDER BY market_id, P1_points, t1_timestamp ASC
    """

    all_outcomes = await _query(db_url, outcomes_sql, params)

    # Group outcomes by P1
    outcomes_by_p1: dict[int, list[dict]] = {}
    for o in all_outcomes:
        p1 = int(o["p1_points"])
        outcomes_by_p1.setdefault(p1, []).append(o)

    # Print header
    print(f"\n{'=' * 110}")
    print(f"  P1 BREAKDOWN: delta={delta}  SL={sl}  f={fraction:.0%}  "
          f"P1={p1_lo}-{p1_hi}¢  time={time_lo}-{time_hi}min")
    print(f"{'=' * 110}\n")

    header = (
        f"  {'P1':>4}  {'Att':>5}  {'Pairs':>5}  {'PairR':>6}  "
        f"{'AvgPnL':>7}  {'TotPnL':>7}  {'Mkts':>5}  "
        f"{'Bankroll':>9}  {'E[logB]':>8}  {'P(win)':>7}  {'95% CI':>20}"
    )
    print(header)
    print(f"  {'-' * 106}")

    totals = {"attempts": 0, "pairs": 0, "total_pnl": 0.0}

    for r in rows:
        p1 = int(r["p1_points"])
        attempts = int(r["attempts"])
        pairs = int(r["pairs"])
        pair_rate = float(r["pair_rate"]) if r["pair_rate"] is not None else 0
        avg_pnl = float(r["avg_pnl"]) if r["avg_pnl"] is not None else 0
        total_pnl = float(r["total_pnl"]) if r["total_pnl"] is not None else 0

        totals["attempts"] += attempts
        totals["pairs"] += pairs
        totals["total_pnl"] += total_pnl

        outcomes = outcomes_by_p1.get(p1, [])
        outcomes.sort(key=lambda o: o["t1_timestamp"])
        n_mkts = len(outcomes)

        if n_mkts >= 2:
            bstats = bootstrap_bankroll_stats(outcomes, fraction)
            bankroll = simulate_compound_bankroll(outcomes, fraction)
            mean_log = bstats["mean_log"]
            p_profit = bstats["p_profit"]
            ci_lo = bstats["ci_lo"]
            ci_hi = bstats["ci_hi"]
        elif n_mkts == 1:
            bankroll = simulate_compound_bankroll(outcomes, fraction)
            mean_log = math.log(max(bankroll, 1e-12))
            p_profit = 1.0 if bankroll > 1.0 else 0.0
            ci_lo = bankroll
            ci_hi = bankroll
        else:
            bankroll = 1.0
            mean_log = 0.0
            p_profit = 0.0
            ci_lo = 1.0
            ci_hi = 1.0

        ci_str = f"[{ci_lo:>8.3f}, {ci_hi:>8.3f}]"
        pnl_sign = "+" if avg_pnl >= 0 else ""

        print(
            f"  {p1:>4}  {attempts:>5}  {pairs:>5}  {pair_rate*100:>5.1f}%  "
            f"{pnl_sign}{avg_pnl:>6.2f}  {total_pnl:>+7.0f}  {n_mkts:>5}  "
            f"{bankroll:>9.3f}  {mean_log:>+8.3f}  {p_profit*100:>6.1f}%  {ci_str}"
        )

    # Totals
    total_att = totals["attempts"]
    total_pairs = totals["pairs"]
    total_pnl = totals["total_pnl"]
    overall_pr = total_pairs / max(total_att, 1)
    overall_avg = total_pnl / max(total_att, 1)

    print(f"  {'-' * 106}")
    print(
        f"  {'ALL':>4}  {total_att:>5}  {total_pairs:>5}  {overall_pr*100:>5.1f}%  "
        f"{'+' if overall_avg >= 0 else ''}{overall_avg:>6.2f}  {total_pnl:>+7.0f}"
    )
    print(f"\n{'=' * 110}\n")


def main():
    load_env_file()
    parser = argparse.ArgumentParser(description="Break down config performance by P1 price")
    parser.add_argument("--db-url", default=None)
    parser.add_argument("--delta", type=int, required=True)
    parser.add_argument("--sl", type=int, required=True)
    parser.add_argument("--p1-lo", type=int, required=True)
    parser.add_argument("--p1-hi", type=int, required=True)
    parser.add_argument("--time-lo", type=int, required=True)
    parser.add_argument("--time-hi", type=int, required=True)
    parser.add_argument("--fraction", type=float, default=0.10)
    parser.add_argument("--markets", default=None)
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
