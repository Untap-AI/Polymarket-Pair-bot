#!/usr/bin/env python3
"""First-per-market P1 range profitability sweep.

For each candidate P1 sub-range within [p1-lo, p1-hi], queries first-per-market
outcomes (filtering to that P1 range first, then DISTINCT ON market_id — matching
how the optimizer and walk forward work). Shows bankroll stats for each range.

Usage:
    python scripts/p1_range_analysis.py \
        --delta 8 --sl 37 --time-lo 10 --time-hi 15 \
        --fraction 0.10 --markets BTC \
        --p1-lo 22 --p1-hi 35
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


def compute_pnl(m: dict) -> float:
    if m["status"] == "completed_paired":
        return float(m["delta_points"])
    return -(float(m["loss_points"]) + float(m.get("taker_fee_points") or 0))


async def fetch_first_per_market_in_range(
    db_url: str,
    delta: int,
    sl: int,
    p1_lo: int,
    p1_hi: int,
    time_lo: int,
    time_hi: int,
    markets: str | None = None,
) -> list[dict]:
    """First attempt per market, filtered to a specific P1 and time range first."""
    params: list = [delta, sl, p1_lo, p1_hi, time_lo, time_hi]
    idx = 7

    markets_filter = ""
    if markets:
        assets = [a.strip().lower() for a in markets.split(",")]
        markets_filter = (
            f"AND market_id IN (SELECT market_id FROM Markets "
            f"WHERE crypto_asset = ANY(${idx}))"
        )
        params.append(assets)

    sql = f"""
        SELECT DISTINCT ON (market_id)
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
          AND time_remaining_at_start <= 900
          AND (100 - P1_points) >= delta_points
          {markets_filter}
        ORDER BY market_id, t1_timestamp ASC
    """

    rows = await _query(db_url, sql, params)
    rows.sort(key=lambda r: r["t1_timestamp"])
    return rows


async def run(args):
    db_url = _resolve_db_url(args)
    delta = args.delta
    sl = args.sl
    p1_lo = args.p1_lo
    p1_hi = args.p1_hi
    time_lo = args.time_lo
    time_hi = args.time_hi
    fraction = args.fraction

    markets_str = args.markets.upper() if args.markets else "all"

    # Build list of all (lo, hi) ranges to sweep
    ranges = []
    for lo in range(p1_lo, p1_hi + 1):
        for hi in range(lo, p1_hi + 1):
            ranges.append((lo, hi))

    print(f"\n{'=' * 120}")
    print(f"  P1 RANGE SWEEP (first-per-market within each range)")
    print(f"  delta={delta}  SL={sl}  f={fraction:.0%}  time={time_lo}-{time_hi}min  "
          f"markets={markets_str}")
    print(f"  Sweeping {len(ranges)} ranges: P1 {p1_lo}-{p1_hi}")
    print(f"{'=' * 120}\n")

    header = (
        f"  {'Range':>10}  {'Mkts':>5}  {'Pairs':>5}  {'PairR':>6}  "
        f"{'AvgPnL':>7}  {'TotPnL':>7}  "
        f"{'Bankroll':>9}  {'E[logB]':>8}  {'P(win)':>7}  {'95% CI':>20}"
    )
    print(header)
    print(f"  {'-' * 118}")

    results = []
    for i, (lo, hi) in enumerate(ranges):
        if (i + 1) % 20 == 0:
            print(f"  ... [{i + 1}/{len(ranges)}] queries done ...")

        outcomes = await fetch_first_per_market_in_range(
            db_url, delta, sl, lo, hi, time_lo, time_hi, args.markets,
        )

        n = len(outcomes)
        if n == 0:
            continue

        pairs = sum(1 for m in outcomes if m["status"] == "completed_paired")
        pair_rate = pairs / n
        pnl_sum = sum(compute_pnl(m) for m in outcomes)
        avg_pnl = pnl_sum / n

        bankroll = simulate_compound_bankroll(outcomes, fraction)
        if n >= 2:
            bs = bootstrap_bankroll_stats(outcomes, fraction)
            mean_log, p_profit = bs["mean_log"], bs["p_profit"]
            ci_lo_v, ci_hi_v = bs["ci_lo"], bs["ci_hi"]
        else:
            mean_log = math.log(max(bankroll, 1e-12))
            p_profit = 1.0 if bankroll > 1.0 else 0.0
            ci_lo_v = ci_hi_v = bankroll

        row = {
            "p1_lo": lo, "p1_hi": hi, "mkts": n, "pairs": pairs,
            "pair_rate": pair_rate, "avg_pnl": avg_pnl, "total_pnl": pnl_sum,
            "bankroll": bankroll, "mean_log": mean_log, "p_profit": p_profit,
            "ci_lo": ci_lo_v, "ci_hi": ci_hi_v,
        }
        results.append(row)

    # Print all results sorted by range
    for r in results:
        rng = f"{r['p1_lo']}-{r['p1_hi']}¢"
        ci_str = f"[{r['ci_lo']:>8.3f}, {r['ci_hi']:>8.3f}]"
        print(
            f"  {rng:>10}  {r['mkts']:>5}  {r['pairs']:>5}  {r['pair_rate']*100:>5.1f}%  "
            f"{r['avg_pnl']:>+7.2f}  {r['total_pnl']:>+7.0f}  "
            f"{r['bankroll']:>9.3f}  {r['mean_log']:>+8.3f}  "
            f"{r['p_profit']*100:>6.1f}%  {ci_str}"
        )

    # Top 10 by E[logB]
    if results:
        print(f"\n  {'=' * 118}")
        print(f"  TOP 10 BY E[logB]")
        print(f"  {'=' * 118}\n")
        print(header)
        print(f"  {'-' * 118}")

        top = sorted(results, key=lambda r: r["mean_log"], reverse=True)[:10]
        for rank, r in enumerate(top, 1):
            rng = f"{r['p1_lo']}-{r['p1_hi']}¢"
            ci_str = f"[{r['ci_lo']:>8.3f}, {r['ci_hi']:>8.3f}]"
            print(
                f"  {rng:>10}  {r['mkts']:>5}  {r['pairs']:>5}  {r['pair_rate']*100:>5.1f}%  "
                f"{r['avg_pnl']:>+7.2f}  {r['total_pnl']:>+7.0f}  "
                f"{r['bankroll']:>9.3f}  {r['mean_log']:>+8.3f}  "
                f"{r['p_profit']*100:>6.1f}%  {ci_str}"
            )

    print(f"\n{'=' * 120}\n")


def main():
    load_env_file()
    parser = argparse.ArgumentParser(
        description="First-per-market P1 range profitability sweep"
    )
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
