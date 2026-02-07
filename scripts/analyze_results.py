#!/usr/bin/env python3
"""CLI analysis report for Polymarket pair measurement data.

Usage:
    python scripts/analyze_results.py                        # all data
    python scripts/analyze_results.py --asset btc            # BTC only
    python scripts/analyze_results.py --after 2026-02-06     # date filter
    python scripts/analyze_results.py --parameter-set 1      # by param set ID
    python scripts/analyze_results.py --db-url 'postgres://…'  # PostgreSQL

The database source is resolved in order:
  1. --db-url flag
  2. DATABASE_URL environment variable
  3. --db flag (SQLite file path, default: data/measurements.db)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402
from src.metrics import (  # noqa: E402
    get_cross_market_consistency,
    get_failure_analysis,
    get_mae_analysis,
    get_near_miss_analysis,
    get_overall_stats,
    get_pair_cost_distribution,
    get_parameter_comparison,
    get_profitability_projection,
    get_spread_analysis,
    get_stats_by_asset,
    get_stats_by_first_leg,
    get_stats_by_market_minute,
    get_stats_by_market_phase,
    get_stats_by_reference_regime,
    get_stats_by_time_bucket,
    get_time_to_pair_distribution,
)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def hr(char: str = "-", width: int = 70) -> str:
    return char * width


def section(title: str) -> str:
    return f"\n{'=' * 70}\n  {title}\n{'=' * 70}"


def pct(val, default: str = "-") -> str:
    if val is None:
        return default
    return f"{val * 100:.1f}%"


def num(val, fmt: str = ".1f", default: str = "-") -> str:
    if val is None:
        return default
    return f"{val:{fmt}}"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

async def run_report(
    db_source: str,
    parameter_set_id: int | None = None,
    crypto_asset: str | None = None,
    date_after: str | None = None,
) -> None:
    filters = []
    if parameter_set_id:
        filters.append(f"param_set={parameter_set_id}")
    if crypto_asset:
        filters.append(f"asset={crypto_asset.upper()}")
    if date_after:
        filters.append(f"after={date_after}")
    filter_str = f" (filters: {', '.join(filters)})" if filters else ""

    print(section(f"POLYMARKET PAIR MEASUREMENT - ANALYSIS REPORT{filter_str}"))

    # --- Overall ---
    stats = await get_overall_stats(db_source, parameter_set_id, crypto_asset, date_after)
    print(f"\n  Total attempts:  {stats.get('total_attempts', 0)}")
    print(f"  Total pairs:     {stats.get('total_pairs', 0)}")
    print(f"  Total failed:    {stats.get('total_failed', 0)}")
    print(f"  Pair rate:       {pct(stats.get('pair_rate'))}")
    print(f"  Avg TTP:         {num(stats.get('avg_ttp'))}s")
    print(f"  Avg cost:        {num(stats.get('avg_cost'))} pts")
    print(f"  Avg profit:      {num(stats.get('avg_profit'))} pts")

    # --- By Asset ---
    by_asset = await get_stats_by_asset(db_source, parameter_set_id)
    if by_asset:
        print(section("BY CRYPTO ASSET"))
        print(f"  {'Asset':<8} {'Attempts':>9} {'Pairs':>7} {'Rate':>7} {'Avg TTP':>9}")
        print(f"  {hr(width=42)}")
        for r in by_asset:
            print(f"  {r['crypto_asset'].upper():<8} {r['attempts']:>9} "
                  f"{r['pairs']:>7} {pct(r['pair_rate']):>7} {num(r['avg_ttp']):>8}s")

    # --- TTP Distribution ---
    ttp_dist = await get_time_to_pair_distribution(db_source, parameter_set_id)
    if ttp_dist:
        print(section("TIME-TO-PAIR DISTRIBUTION"))
        print(f"  {'Bucket':<12} {'Count':>7} {'Avg Profit':>11}")
        print(f"  {hr(width=32)}")
        for r in ttp_dist:
            print(f"  {r['bucket']:<12} {r['count']:>7} "
                  f"{num(r['avg_profit']):>10} pts")

    # --- By First Leg ---
    by_leg = await get_stats_by_first_leg(db_source, parameter_set_id)
    if by_leg:
        print(section("BY FIRST LEG SIDE"))
        print(f"  {'Side':<8} {'Attempts':>9} {'Pairs':>7} {'Rate':>7} "
              f"{'Avg TTP':>9} {'Profit':>8} {'Avg MAE':>8}")
        print(f"  {hr(width=60)}")
        for r in by_leg:
            print(f"  {r['first_leg_side']:<8} {r['attempts']:>9} "
                  f"{r['pairs']:>7} {pct(r['pair_rate']):>7} {num(r['avg_ttp']):>8}s "
                  f"{num(r.get('avg_profit')):>7}p {num(r.get('avg_mae')):>7}p")

    # --- By Market Phase ---
    by_phase = await get_stats_by_market_phase(db_source, parameter_set_id)
    if by_phase:
        print(section("BY MARKET PHASE"))
        print(f"  {'Phase':<20} {'Attempts':>9} {'Pairs':>7} {'Rate':>7}")
        print(f"  {hr(width=45)}")
        for r in by_phase:
            print(f"  {r['phase']:<20} {r['attempts']:>9} "
                  f"{r['pairs']:>7} {pct(r['pair_rate']):>7}")

    # --- By Market Minute (5 x 3-min buckets) ---
    by_minute = await get_stats_by_market_minute(db_source, parameter_set_id)
    if by_minute:
        print(section("PAIR RATE BY MARKET MINUTE (5 x 3-min buckets)"))
        print(f"  {'Bucket':<12} {'Attempts':>9} {'Pairs':>7} {'Rate':>7} {'Avg TTP':>9} {'Avg Profit':>11}")
        print(f"  {hr(width=57)}")
        for r in by_minute:
            print(f"  {r['bucket']:<12} {r['attempts']:>9} "
                  f"{r['pairs']:>7} {pct(r['pair_rate']):>7} "
                  f"{num(r['avg_ttp']):>8}s {num(r['avg_profit']):>10} pts")

    # --- By Time Remaining Bucket ---
    by_bucket = await get_stats_by_time_bucket(db_source, parameter_set_id)
    if by_bucket:
        print(section("BY TIME REMAINING AT ENTRY"))
        print(f"  {'Bucket':<12} {'Attempts':>9} {'Pairs':>7} {'Rate':>7} "
              f"{'Avg TTP':>9} {'Profit':>8} {'Avg MAE':>8}")
        print(f"  {hr(width=62)}")
        for r in by_bucket:
            print(f"  {r['bucket']:<12} {r['attempts']:>9} "
                  f"{r['pairs']:>7} {pct(r['pair_rate']):>7} "
                  f"{num(r['avg_ttp']):>8}s {num(r.get('avg_profit')):>7}p "
                  f"{num(r.get('avg_mae')):>7}p")

    # --- MAE Analysis ---
    mae = await get_mae_analysis(db_source, parameter_set_id)
    if mae.get("overall", {}).get("total"):
        print(section("MAX ADVERSE EXCURSION (MAE) ANALYSIS"))
        ov = mae["overall"]
        print(f"  Avg MAE:  {num(ov.get('avg_mae'))} pts")
        print(f"  Max MAE:  {num(ov.get('max_mae'), '.0f')} pts")
        print(f"  Min MAE:  {num(ov.get('min_mae'), '.0f')} pts")
        if mae.get("by_outcome"):
            print(f"\n  {'Outcome':<20} {'Count':>7} {'Avg MAE':>9} {'Max MAE':>9}")
            print(f"  {hr(width=47)}")
            for r in mae["by_outcome"]:
                print(f"  {r['status']:<20} {r['count']:>7} "
                      f"{num(r['avg_mae']):>8}p {num(r['max_mae'], '.0f'):>8}p")
        if mae.get("buckets"):
            print(f"\n  {'MAE Bucket':<16} {'Count':>7} {'Pair Rate':>10}")
            print(f"  {hr(width=35)}")
            for r in mae["buckets"]:
                print(f"  {r['bucket']:<16} {r['count']:>7} {pct(r['pair_rate']):>10}")

    # --- Spread Analysis ---
    spread = await get_spread_analysis(db_source, parameter_set_id)
    if spread.get("entry", {}).get("avg_yes_spread_entry") is not None:
        print(section("SPREAD AT ENTRY / EXIT"))
        ent = spread["entry"]
        ext = spread["exit"]
        print(f"  Entry spreads (all attempts):")
        print(f"    YES:  avg={num(ent.get('avg_yes_spread_entry'))}  "
              f"min={num(ent.get('min_yes_spread_entry'), '.0f')}  "
              f"max={num(ent.get('max_yes_spread_entry'), '.0f')}")
        print(f"    NO:   avg={num(ent.get('avg_no_spread_entry'))}  "
              f"min={num(ent.get('min_no_spread_entry'), '.0f')}  "
              f"max={num(ent.get('max_no_spread_entry'), '.0f')}")
        if ext.get("avg_yes_spread_exit") is not None:
            print(f"  Exit spreads (paired only):")
            print(f"    YES:  avg={num(ext.get('avg_yes_spread_exit'))}  "
                  f"max={num(ext.get('max_yes_spread_exit'), '.0f')}")
            print(f"    NO:   avg={num(ext.get('avg_no_spread_exit'))}  "
                  f"max={num(ext.get('max_no_spread_exit'), '.0f')}")
        if spread.get("by_combined_spread"):
            print(f"\n  Pair rate by combined entry spread:")
            print(f"  {'Spread':<18} {'Attempts':>9} {'Pairs':>7} {'Rate':>7} {'Avg TTP':>9}")
            print(f"  {hr(width=52)}")
            for r in spread["by_combined_spread"]:
                print(f"  {r['combined_spread_bucket']:<18} {r['attempts']:>9} "
                      f"{r['pairs']:>7} {pct(r['pair_rate']):>7} {num(r['avg_ttp']):>8}s")

    # --- By Reference Regime ---
    by_regime = await get_stats_by_reference_regime(db_source, parameter_set_id)
    if by_regime:
        print(section("BY REFERENCE PRICE REGIME"))
        print(f"  {'Regime':<25} {'Attempts':>9} {'Pairs':>7} {'Rate':>7}")
        print(f"  {hr(width=50)}")
        for r in by_regime:
            print(f"  {r['regime']:<25} {r['attempts']:>9} "
                  f"{r['pairs']:>7} {pct(r['pair_rate']):>7}")

    # --- Pair Cost ---
    by_cost = await get_pair_cost_distribution(db_source, parameter_set_id)
    if by_cost:
        print(section("PAIR COST DISTRIBUTION"))
        print(f"  {'Bucket':<18} {'Count':>7} {'Avg Profit':>11} {'Avg TTP':>9}")
        print(f"  {hr(width=47)}")
        for r in by_cost:
            print(f"  {r['bucket']:<18} {r['count']:>7} "
                  f"{num(r['avg_profit']):>10} pts {num(r['avg_ttp']):>8}s")

    # --- Failure Analysis ---
    failures = await get_failure_analysis(db_source, parameter_set_id)
    if failures.get("total_failed"):
        print(section("FAILURE ANALYSIS"))
        print(f"  Total failed:       {failures.get('total_failed', 0)}")
        print(f"  Avg closest approach: {num(failures.get('avg_closest'))} pts")
        if failures.get("by_reason"):
            print(f"\n  {'Reason':<25} {'Count':>7} {'Avg Closest':>12}")
            print(f"  {hr(width=46)}")
            for r in failures["by_reason"]:
                print(f"  {(r['fail_reason'] or 'unknown'):<25} {r['count']:>7} "
                      f"{num(r['avg_closest_approach']):>11} pts")

    # --- Near Miss ---
    near = await get_near_miss_analysis(db_source, parameter_set_id)
    if near.get("total"):
        print(section("NEAR MISS ANALYSIS"))
        print(f"  Frustration rate (within 2pts): {pct(near.get('frustration_rate'))}")
        print(f"  Avg closest approach:           {num(near.get('avg_closest'))} pts")
        if near.get("proximity_buckets"):
            print(f"\n  {'Proximity':<15} {'Count':>7}")
            print(f"  {hr(width=24)}")
            for r in near["proximity_buckets"]:
                print(f"  {r['proximity']:<15} {r['count']:>7}")

    # --- Cross-Market Consistency ---
    consistency = await get_cross_market_consistency(db_source, parameter_set_id)
    if consistency:
        rates = [r["pair_rate"] for r in consistency if r["pair_rate"] is not None]
        if rates:
            avg_rate = sum(rates) / len(rates)
            print(section("CROSS-MARKET CONSISTENCY"))
            print(f"  Markets with data: {len(consistency)}")
            print(f"  Avg pair rate:     {pct(avg_rate)}")
            if len(rates) > 1:
                variance = sum((r - avg_rate) ** 2 for r in rates) / len(rates)
                print(f"  Std deviation:     {pct(variance ** 0.5)}")
            print(f"\n  Top 5 markets:")
            for r in consistency[:5]:
                print(f"    {r['market_id']}: {r['attempts']} att, {pct(r['pair_rate'])}")
            if len(consistency) > 5:
                print(f"  Bottom 5 markets:")
                for r in consistency[-5:]:
                    print(f"    {r['market_id']}: {r['attempts']} att, {pct(r['pair_rate'])}")

    # --- Parameter Comparison ---
    param_cmp = await get_parameter_comparison(db_source)
    if len(param_cmp) > 1:
        print(section("PARAMETER SET COMPARISON"))
        print(f"  {'Name':<15} {'S0':>4} {'d':>4} {'Att':>6} {'Pairs':>6} "
              f"{'Rate':>7} {'TTP':>7} {'Profit':>7}")
        print(f"  {hr(width=58)}")
        for r in param_cmp:
            print(f"  {(r['name'] or '?'):<15} {r['S0_points'] or 0:>4} "
                  f"{r['delta_points'] or 0:>4} {r['attempts'] or 0:>6} "
                  f"{r['pairs'] or 0:>6} {pct(r['pair_rate']):>7} "
                  f"{num(r['avg_ttp']):>6}s {num(r['avg_profit']):>6}p")

    # --- Profitability Projection ---
    proj = await get_profitability_projection(db_source, parameter_set_id)
    if proj.get("pair_rate"):
        print(section("PROFITABILITY PROJECTION"))
        print(f"  Observed pair rate:     {pct(proj['pair_rate'])}")
        print(f"  Avg profit per pair:    {num(proj['avg_profit_points'])} pts")
        print(f"  Exit loss (assumed):    {proj['exit_loss_points']} pts")
        print(f"  Breakeven pair rate:    {pct(proj['breakeven_pair_rate'])}")
        print(f"  EV per attempt:         {num(proj['ev_per_attempt'], '.3f')} pts "
              f"(${proj['ev_per_attempt']/100:.5f})")
        print(f"  {hr(width=45)}")
        print(f"  Avg attempts/market:    {num(proj['avg_attempts_per_market'])}")
        print(f"  Markets/day (projected):{proj['markets_per_day']:>5}")
        print(f"  Attempts/day:           {num(proj['attempts_per_day'], '.0f')}")
        print(f"  Daily EV:               {num(proj['daily_ev_points'], '.1f')} pts "
              f"(${num(proj['daily_ev_dollars'], '.2f')})")
        print(f"  Monthly EV:             {num(proj['monthly_ev_points'], '.0f')} pts "
              f"(${num(proj['monthly_ev_dollars'], '.2f')})")

    print(f"\n{'=' * 70}")
    print("  Report complete.")
    print(f"{'=' * 70}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _resolve_db_source(args) -> str:
    """Return the database source string (URL or file path)."""
    # 1. Explicit --db-url flag
    if args.db_url:
        return args.db_url
    # 2. DATABASE_URL env var
    env_url = os.environ.get("DATABASE_URL")
    if env_url:
        return env_url
    # 3. SQLite file path
    return args.db


def main():
    # Load .env file if it exists
    load_env_file()
    
    parser = argparse.ArgumentParser(
        description="Analyze Polymarket pair measurement results"
    )
    parser.add_argument(
        "--db", default="data/measurements.db",
        help="Path to SQLite database (default: data/measurements.db)",
    )
    parser.add_argument(
        "--db-url", default=None,
        help="PostgreSQL connection URL (overrides --db and DATABASE_URL)",
    )
    parser.add_argument(
        "--asset", default=None,
        help="Filter by crypto asset (btc, eth, sol, xrp)",
    )
    parser.add_argument(
        "--parameter-set", type=int, default=None,
        help="Filter by parameter_set_id",
    )
    parser.add_argument(
        "--after", default=None,
        help="Filter attempts after date (YYYY-MM-DD)",
    )
    args = parser.parse_args()

    db_source = _resolve_db_source(args)

    # Only check file existence for SQLite paths
    if "postgres" not in db_source.lower() and not os.path.exists(db_source):
        print(f"Database not found: {db_source}")
        sys.exit(1)

    asyncio.run(run_report(
        db_source=db_source,
        parameter_set_id=args.parameter_set,
        crypto_asset=args.asset,
        date_after=args.after,
    ))


if __name__ == "__main__":
    main()
