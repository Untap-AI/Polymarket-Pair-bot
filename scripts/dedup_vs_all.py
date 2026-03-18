#!/usr/bin/env python3
"""Dedup vs All-Attempts Comparison

Tests whether first-attempt-per-market dedup changes config rankings,
or just absolute numbers. If rankings are preserved, the fast all-attempts
method can be used for parameter search (skip the expensive DISTINCT ON).

For each config, computes metrics two ways:
  1. Dedup: DISTINCT ON (market_id) — first attempt per market (current method)
  2. All:   Every attempt, no dedup (fast method)

Then compares: do the same configs rank in the same order by E[logB]?

Usage:
    python scripts/dedup_vs_all.py \
      --config "delta=14,sl=33,p1_lo=77,p1_hi=78,time_lo=10,time_hi=13,f=0.10" \
      --config "delta=12,sl=31,p1_lo=52,p1_hi=60,time_lo=3,time_hi=4,f=0.10" \
      --markets ETH

    python scripts/dedup_vs_all.py \
      --config "delta=14,sl=33,p1_lo=50,p1_hi=90,time_lo=1,time_hi=15,f=0.10" \
      --markets ETH,BTC --after 2026-02-15
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from typing import Optional

import numpy as np

# ─── path setup ──────────────────────────────────────────────────────────────
_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, _SCRIPTS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.config import load_env_file   # noqa: E402
import optimize_params as _opt         # noqa: E402
import duckdb_conn as _duck            # noqa: E402

# ─── borrow helpers ──────────────────────────────────────────────────────────
_query                     = _opt._query
_TAKER_FEE_SQL             = _opt._TAKER_FEE_SQL
simulate_compound_bankroll = _opt.simulate_compound_bankroll
bootstrap_bankroll_stats   = _opt.bootstrap_bankroll_stats
sl_str                     = _opt.sl_str
time_range_str             = _opt.time_range_str


# =============================================================================
# Config parsing
# =============================================================================

def _parse_config_str(s: str) -> dict:
    kv: dict[str, str] = {}
    for part in s.split(","):
        part = part.strip()
        if "=" not in part:
            raise ValueError(f"Invalid token in config string (expected key=value): {part!r}")
        k, v = part.split("=", 1)
        kv[k.strip().lower()] = v.strip()

    required = {"delta", "sl", "p1_lo", "p1_hi", "time_lo", "time_hi"}
    missing = required - kv.keys()
    if missing:
        raise ValueError(f"Config string missing required keys: {', '.join(sorted(missing))}")

    return {
        "delta":     int(kv["delta"]),
        "stop_loss": int(kv["sl"]),
        "p1_lo":     int(kv["p1_lo"]),
        "p1_hi":     int(kv["p1_hi"]),
        "time_lo":   int(kv["time_lo"]),
        "time_hi":   int(kv["time_hi"]),
        "fraction":  float(kv.get("f", "0.15")),
    }


# =============================================================================
# WHERE clause builder
# =============================================================================

def _cfg_filter(
    cfg:        dict,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None,
    markets:    Optional[list[str]] = None,
) -> tuple[str, list]:
    parts: list[str] = []
    params: list = []
    idx = 1

    parts.append(f"delta_points = ${idx}")
    params.append(cfg["delta"])
    idx += 1

    parts.append("S0_points = 1")

    if cfg.get("stop_loss") is not None:
        parts.append(f"stop_loss_threshold_points = ${idx}")
        params.append(cfg["stop_loss"])
        idx += 1
    else:
        parts.append("stop_loss_threshold_points IS NULL")

    if markets:
        parts.append(f"crypto_asset = ANY(${idx})")
        params.append(markets)
        idx += 1

    if date_after:
        parts.append(f"t1_timestamp >= ${idx}")
        parts.append(f"ts >= ${idx}::timestamp")
        params.append(date_after)
        idx += 1
    if date_before:
        parts.append(f"t1_timestamp < ${idx}")
        parts.append(f"ts < ${idx}::timestamp")
        params.append(date_before)
        idx += 1

    parts.append(f"P1_points BETWEEN ${idx} AND ${idx + 1}")
    params.append(cfg["p1_lo"])
    params.append(cfg["p1_hi"])
    idx += 2

    parts.append(f"time_remaining_at_start > ${idx} AND time_remaining_at_start <= ${idx + 1}")
    params.append((cfg["time_lo"] - 1) * 60)
    params.append(cfg["time_hi"] * 60)
    idx += 2

    # Match partial index condition on idx_attempts_stage3 (migration 013)
    parts.append("time_remaining_at_start <= 900")
    parts.append("status IN ('completed_paired', 'completed_failed')")

    return "WHERE " + " AND ".join(parts), params


# =============================================================================
# Data fetching — both modes
# =============================================================================

_SELECT_COLS = f"""
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
"""


async def fetch_deduped(
    db_url: str, cfg: dict,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None,
    markets: Optional[list[str]] = None,
    use_parquet: bool = False,
) -> list[dict]:
    """First attempt per market (current method)."""
    full_where, params = _cfg_filter(cfg, date_after, date_before, markets)
    sql = f"""
        SELECT DISTINCT ON (market_id)
            {_SELECT_COLS}
        FROM Attempts
        {full_where}
        ORDER BY market_id, t1_timestamp ASC
    """
    if use_parquet:
        conn = _duck.get_connection()
        adapted = _duck.adapt_sql_for_duckdb(sql)
        rows = _duck.query(conn, adapted, params)
    else:
        rows = await _query(db_url, sql, params)
    rows.sort(key=lambda r: r["t1_timestamp"])
    return rows


async def fetch_all(
    db_url: str, cfg: dict,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None,
    markets: Optional[list[str]] = None,
    use_parquet: bool = False,
) -> list[dict]:
    """All attempts, no dedup."""
    full_where, params = _cfg_filter(cfg, date_after, date_before, markets)
    sql = f"""
        SELECT
            {_SELECT_COLS}
        FROM Attempts
        {full_where}
        ORDER BY t1_timestamp ASC
    """
    if use_parquet:
        conn = _duck.get_connection()
        adapted = _duck.adapt_sql_for_duckdb(sql)
        return _duck.query(conn, adapted, params)
    rows = await _query(db_url, sql, params)
    return rows


# =============================================================================
# Metrics
# =============================================================================

def _net_pnl(m: dict) -> float:
    if m["status"] == "completed_paired":
        return float(m["delta_points"])
    return -(float(m["loss_points"]) + float(m.get("taker_fee_points") or 0))


def compute_metrics(rows: list[dict], fraction: float) -> dict:
    n = len(rows)
    if n == 0:
        return {"count": 0, "pair_rate": 0, "avg_pnl": 0, "bankroll": 1.0,
                "mean_log": 0, "p_profit": 0, "ci_lo": 1, "ci_hi": 1, "median": 1}

    pairs = sum(1 for r in rows if r["status"] == "completed_paired")
    pair_rate = pairs / n
    pnls = [_net_pnl(r) for r in rows]
    avg_pnl = sum(pnls) / n

    broll = simulate_compound_bankroll(rows, fraction)
    bstats = bootstrap_bankroll_stats(rows, fraction)

    return {
        "count": n,
        "pairs": pairs,
        "pair_rate": pair_rate,
        "avg_pnl": avg_pnl,
        "bankroll": broll,
        "mean_log": bstats["mean_log"],
        "p_profit": bstats["p_profit"],
        "ci_lo": bstats["ci_lo"],
        "ci_hi": bstats["ci_hi"],
        "median": bstats["median"],
    }


# =============================================================================
# Output
# =============================================================================

def _cfg_label(cfg: dict) -> str:
    return (f"d={cfg['delta']} sl={sl_str(cfg.get('stop_loss'))} "
            f"P1={cfg['p1_lo']}-{cfg['p1_hi']} "
            f"t={cfg['time_lo']}-{cfg['time_hi']}")


def print_results(
    configs: list[dict],
    dedup_metrics: list[dict],
    all_metrics: list[dict],
) -> None:
    print()
    print("=" * 130)
    print("  DEDUP vs ALL-ATTEMPTS COMPARISON")
    print("=" * 130)

    # ── Table 1: Side-by-side metrics ────────────────────────────────────────
    print(f"\n  {'#':>3}  {'Config':<35}  "
          f"{'--- Dedup (first/market) ---':^42}  "
          f"{'--- All Attempts ---':^42}")
    print(f"  {'':>3}  {'':35}  "
          f"{'N':>5} {'PairR':>7} {'AvgPnL':>8} {'E[logB]':>9} {'P(win)':>7}  "
          f"{'N':>5} {'PairR':>7} {'AvgPnL':>8} {'E[logB]':>9} {'P(win)':>7}")
    print(f"  {'-' * 126}")

    for i, (cfg, dm, am) in enumerate(zip(configs, dedup_metrics, all_metrics), 1):
        label = _cfg_label(cfg)
        print(
            f"  {i:>3}  {label:<35}  "
            f"{dm['count']:>5} {dm['pair_rate']*100:>6.1f}% {dm['avg_pnl']:>+8.2f} "
            f"{dm['mean_log']:>+9.3f} {dm['p_profit']*100:>6.1f}%  "
            f"{am['count']:>5} {am['pair_rate']*100:>6.1f}% {am['avg_pnl']:>+8.2f} "
            f"{am['mean_log']:>+9.3f} {am['p_profit']*100:>6.1f}%"
        )

    # ── Table 2: Rank comparison ─────────────────────────────────────────────
    if len(configs) >= 2:
        dedup_elogs = [m["mean_log"] for m in dedup_metrics]
        all_elogs = [m["mean_log"] for m in all_metrics]

        # Rank (1 = best)
        dedup_order = np.argsort(dedup_elogs)[::-1]
        all_order = np.argsort(all_elogs)[::-1]
        dedup_ranks = np.empty(len(configs), dtype=int)
        all_ranks = np.empty(len(configs), dtype=int)
        for rank, idx in enumerate(dedup_order, 1):
            dedup_ranks[idx] = rank
        for rank, idx in enumerate(all_order, 1):
            all_ranks[idx] = rank

        print(f"\n  Ranking by E[logB]:")
        print(f"  {'#':>3}  {'Config':<35}  {'Dedup Rank':>10}  {'All Rank':>10}  {'Diff':>6}")
        print(f"  {'-' * 70}")

        rank_diffs = []
        for i, cfg in enumerate(configs):
            diff = abs(int(dedup_ranks[i]) - int(all_ranks[i]))
            rank_diffs.append(diff)
            label = _cfg_label(cfg)
            print(f"  {i+1:>3}  {label:<35}  {dedup_ranks[i]:>10}  {all_ranks[i]:>10}  {diff:>+6}")

        # Spearman rank correlation
        n = len(configs)
        if n >= 3:
            d_sq_sum = sum((int(dedup_ranks[i]) - int(all_ranks[i])) ** 2 for i in range(n))
            spearman = 1 - (6 * d_sq_sum) / (n * (n**2 - 1))
        else:
            spearman = 1.0 if all(dedup_ranks[i] == all_ranks[i] for i in range(n)) else 0.0

        same_top = dedup_ranks[all_order[0]] == 1
        max_diff = max(rank_diffs)

        print(f"\n  Spearman rank correlation: {spearman:+.3f}")
        print(f"  Same #1 config:           {'YES' if same_top else 'NO'}")
        print(f"  Max rank difference:       {max_diff}")

        if spearman > 0.9:
            verdict = "Rankings nearly identical — all-attempts is a safe proxy for search."
        elif spearman > 0.7:
            verdict = "Rankings mostly preserved — all-attempts is usable for coarse search, verify top-K with dedup."
        elif spearman > 0.4:
            verdict = "Rankings moderately correlated — all-attempts gives a rough signal but dedup changes ordering."
        else:
            verdict = "Rankings diverge — dedup materially changes which configs look best. Can't skip it."

        print(f"\n  Verdict: {verdict}")

    elif len(configs) == 1:
        # Single config — just show the delta
        dm = dedup_metrics[0]
        am = all_metrics[0]
        print(f"\n  Single config comparison:")
        print(f"    Pair rate delta:  {(am['pair_rate'] - dm['pair_rate']) * 100:+.1f}pp")
        print(f"    Avg PnL delta:    {am['avg_pnl'] - dm['avg_pnl']:+.2f}")
        print(f"    E[logB] delta:    {am['mean_log'] - dm['mean_log']:+.3f}")
        print(f"    All/Dedup ratio:  {am['count']}/{dm['count']} = {am['count']/max(dm['count'],1):.2f}x attempts")

        if abs(am["pair_rate"] - dm["pair_rate"]) < 0.02 and abs(am["mean_log"] - dm["mean_log"]) < 0.1:
            print(f"\n  Verdict: Metrics are close — dedup doesn't change the picture much for this config.")
        else:
            print(f"\n  Verdict: Metrics diverge — multiple attempts per market shift the distribution.")

    print()


# =============================================================================
# Main
# =============================================================================

async def run(
    db_url:      str,
    configs:     list[dict],
    date_after:  Optional[str] = None,
    date_before: Optional[str] = None,
    markets:     Optional[list[str]] = None,
    use_parquet: bool = False,
) -> None:
    markets_str = ", ".join(a.upper() for a in sorted(markets)) if markets else "all"
    print(f"\nDedup vs All-Attempts  (markets={markets_str}, {len(configs)} configs)", flush=True)
    if date_after:
        print(f"  After: {date_after}", flush=True)
    if date_before:
        print(f"  Before: {date_before}", flush=True)
    print(flush=True)

    dedup_metrics: list[dict] = []
    all_metrics: list[dict] = []

    for i, cfg in enumerate(configs, 1):
        label = _cfg_label(cfg)
        print(f"  [{i}/{len(configs)}] {label}", flush=True)

        print(f"    Querying...", flush=True)
        d_rows, a_rows = await asyncio.gather(
            fetch_deduped(db_url, cfg, date_after, date_before, markets, use_parquet),
            fetch_all(db_url, cfg, date_after, date_before, markets, use_parquet),
        )
        print(f"    Dedup: {len(d_rows)} rows  |  All: {len(a_rows)} rows", flush=True)

        print(f"    Computing metrics...", flush=True)
        dm = compute_metrics(d_rows, cfg["fraction"])
        am = compute_metrics(a_rows, cfg["fraction"])
        dedup_metrics.append(dm)
        all_metrics.append(am)
        print(f"    Done.", flush=True)

    print_results(configs, dedup_metrics, all_metrics)


# =============================================================================
# CLI
# =============================================================================

def _resolve_db_url(args) -> str:
    if args.db_url:
        return args.db_url
    url = os.environ.get("DATABASE_URL_SESSION") or os.environ.get("DATABASE_URL")
    if url:
        return url
    print("ERROR: No database URL. Set DATABASE_URL or pass --db-url.", file=sys.stderr)
    sys.exit(1)


def _resolve_markets(args) -> Optional[list[str]]:
    raw = getattr(args, "markets", None) or ""
    if not raw:
        return None
    assets = [a.strip().lower() for a in raw.split(",") if a.strip()]
    return assets or None


def main() -> None:
    load_env_file()
    parser = argparse.ArgumentParser(
        description="Compare dedup (first attempt per market) vs all-attempts metrics"
    )
    parser.add_argument("--config", action="append", required=True,
                        help="Config string (can pass multiple). Format: "
                             "delta=<int>,sl=<int>,p1_lo=<int>,p1_hi=<int>,"
                             "time_lo=<int>,time_hi=<int>[,f=<float>]")
    parser.add_argument("--markets", default=None,
                        help="Comma-separated crypto assets (default: all)")
    parser.add_argument("--after", default=None,
                        help="Only use data after this date (YYYY-MM-DD)")
    parser.add_argument("--before", default=None,
                        help="Only use data before this date (YYYY-MM-DD)")
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument("--use-parquet", action="store_true",
                        help="Query DuckDB/Parquet instead of Postgres "
                             "(requires: python scripts/export_to_parquet.py --days N first)")
    args = parser.parse_args()

    configs = [_parse_config_str(s) for s in args.config]
    db_url = _resolve_db_url(args)
    markets = _resolve_markets(args)

    asyncio.run(run(
        db_url=db_url,
        configs=configs,
        date_after=args.after,
        date_before=args.before,
        markets=markets,
        use_parquet=args.use_parquet,
    ))


if __name__ == "__main__":
    main()
