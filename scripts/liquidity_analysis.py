#!/usr/bin/env python3
"""Liquidity Exploration — does liquidity at entry predict pair rate and profitability?

Buckets historical attempts by best ask size (both first-leg and second-leg)
and computes per-bucket bankroll metrics side by side, so you can see which
leg's liquidity is the stronger predictor.

Usage:
    python scripts/liquidity_analysis.py \
      --config "delta=14,sl=33,p1_lo=77,p1_hi=78,time_lo=10,time_hi=13,f=0.10" \
      --markets ETH

    python scripts/liquidity_analysis.py \
      --config "delta=14,sl=33,p1_lo=77,p1_hi=78,time_lo=10,time_hi=13,f=0.10" \
      --markets ETH --buckets 3 --after 2026-02-15
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

# ─── borrow helpers from the optimizer ────────────────────────────────────────
_query                     = _opt._query
_TAKER_FEE_SQL             = _opt._TAKER_FEE_SQL
simulate_compound_bankroll = _opt.simulate_compound_bankroll
bootstrap_bankroll_stats   = _opt.bootstrap_bankroll_stats
sl_str                     = _opt.sl_str
time_range_str             = _opt.time_range_str


# =============================================================================
# Config parsing (same format as walk_forward.py)
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
# WHERE clause builder (adapted from walk_forward._cfg_filter_range)
# =============================================================================

def _cfg_filter(
    cfg:        dict,
    date_after: Optional[str] = None,
    date_before: Optional[str] = None,
    markets:    Optional[list[str]] = None,
) -> tuple[str, list]:
    """WHERE + params for a specific config, with optional date bounds."""
    parts: list[str] = []
    params: list = []
    idx = 1

    # Equality filters (index-friendly order)
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

    # Range filters
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

    # CEIL(sec/60) = M  ⟺  sec ∈ ((M-1)*60, M*60]
    parts.append(f"time_remaining_at_start > ${idx} AND time_remaining_at_start <= ${idx + 1}")
    params.append((cfg["time_lo"] - 1) * 60)
    params.append(cfg["time_hi"] * 60)
    idx += 2

    parts.append("status IN ('completed_paired', 'completed_failed')")

    return "WHERE " + " AND ".join(parts), params


# =============================================================================
# Data fetching
# =============================================================================

async def fetch_attempts_with_liquidity(
    db_url:      str,
    cfg:         dict,
    date_after:  Optional[str] = None,
    date_before: Optional[str] = None,
    markets:     Optional[list[str]] = None,
    use_parquet: bool = False,
) -> list[dict]:
    """Fetch all matching attempts with liquidity columns, deduped to first per market."""
    full_where, params = _cfg_filter(cfg, date_after, date_before, markets)
    sql = f"""
        SELECT DISTINCT ON (market_id)
            market_id,
            t1_timestamp,
            status,
            P1_points,
            delta_points,
            first_leg_side,
            yes_best_ask_size,
            no_best_ask_size,
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


# =============================================================================
# Liquidity computation — supports both first-leg and second-leg
# =============================================================================

def _extract_ask(row: dict, leg: str) -> float | None:
    """Extract the relevant ask size for a given leg.

    first-leg:  the token we entered on (YES side → yes_best_ask_size)
    second-leg: the token the pair needs to fill (YES side → no_best_ask_size)
    """
    side = row.get("first_leg_side")
    if leg == "first":
        # First leg: same side as first_leg_side
        if side and side.upper() == "YES":
            val = row.get("yes_best_ask_size")
        else:
            val = row.get("no_best_ask_size")
    else:
        # Second leg: opposite side
        if side and side.upper() == "YES":
            val = row.get("no_best_ask_size")
        else:
            val = row.get("yes_best_ask_size")
    return float(val) if val is not None else None


def split_by_liquidity(rows: list[dict], leg: str) -> tuple[list[dict], int]:
    """Add 'relevant_ask' field for the given leg. Returns (rows_with_data, null_count)."""
    with_data = []
    null_count = 0
    for r in rows:
        val = _extract_ask(r, leg)
        if val is not None:
            # Copy so first-leg and second-leg analyses don't clobber each other
            row = dict(r)
            row["relevant_ask"] = val
            with_data.append(row)
        else:
            null_count += 1
    return with_data, null_count


def bucket_by_quantile(rows: list[dict], n_buckets: int) -> list[dict]:
    """Split rows into N equal-count quantile buckets.

    Returns a list of bucket dicts: {label, lo, hi, rows}.
    """
    rows_sorted = sorted(rows, key=lambda r: r["relevant_ask"])
    n = len(rows_sorted)
    bucket_size = n / n_buckets
    buckets = []

    for i in range(n_buckets):
        start = int(round(i * bucket_size))
        end = int(round((i + 1) * bucket_size))
        bucket_rows = rows_sorted[start:end]
        if not bucket_rows:
            continue
        lo = bucket_rows[0]["relevant_ask"]
        hi = bucket_rows[-1]["relevant_ask"]
        label = f"Q{i + 1}"
        buckets.append({
            "label": label,
            "lo": lo,
            "hi": hi,
            "rows": bucket_rows,
        })
    return buckets


# =============================================================================
# Per-bucket analysis
# =============================================================================

def _net_pnl(m: dict) -> float:
    if m["status"] == "completed_paired":
        return float(m["delta_points"])
    return -(float(m["loss_points"]) + float(m.get("taker_fee_points") or 0))


def analyze_bucket(rows: list[dict], fraction: float) -> dict:
    """Compute all metrics for a bucket of outcomes."""
    n = len(rows)
    pairs = sum(1 for r in rows if r["status"] == "completed_paired")
    pair_rate = pairs / n if n else 0.0
    pnls = [_net_pnl(r) for r in rows]
    avg_pnl = sum(pnls) / n if n else 0.0

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
# Summary stats for a single leg analysis
# =============================================================================

def _summarize(bucket_metrics: list[dict], buckets: list[dict]) -> dict:
    """Compute summary stats (spread, monotonicity, verdict) for one leg."""
    e_logs = [m["mean_log"] for m in bucket_metrics]
    best_idx = int(np.argmax(e_logs))
    worst_idx = int(np.argmin(e_logs))
    spread = max(e_logs) - min(e_logs)

    violations = 0
    for i in range(1, len(e_logs)):
        if e_logs[i] < e_logs[i - 1]:
            violations += 1
    monotonic = violations <= 1

    if spread > 0.15:
        signal = "strong"
    elif spread > 0.05:
        signal = "moderate"
    else:
        signal = "weak/no"

    return {
        "e_logs": e_logs,
        "best_idx": best_idx,
        "worst_idx": worst_idx,
        "spread": spread,
        "violations": violations,
        "monotonic": monotonic,
        "signal": signal,
    }


# =============================================================================
# Output — side-by-side tables
# =============================================================================

def _print_leg_table(
    leg_label: str,
    buckets: list[dict],
    bucket_metrics: list[dict],
    all_metrics: dict,
    total_rows: int,
    null_count: int,
) -> None:
    """Print the outcomes table for one leg."""
    n_with_data = total_rows - null_count

    print(f"\n  {leg_label} Ask Size  "
          f"({n_with_data}/{total_rows} rows, {n_with_data / total_rows * 100:.0f}% coverage)")
    print(f"  {'-' * 100}")
    print(
        f"    {'Bucket':<8} {'Range':<16} {'Mkts':>5} {'PairR':>7} "
        f"{'AvgPnL':>8} {'Bankroll':>9} {'E[logB]':>9} "
        f"{'P(win)':>7} {'95% CI':>22}"
    )

    def _row(label: str, range_str: str, m: dict) -> None:
        ci = f"[{m['ci_lo']:.3f}, {m['ci_hi']:.3f}]"
        print(
            f"    {label:<8} {range_str:<16} {m['count']:>5} "
            f"{m['pair_rate'] * 100:>6.1f}% "
            f"{m['avg_pnl']:>+8.2f} "
            f"{m['bankroll']:>9.3f} "
            f"{m['mean_log']:>+9.3f} "
            f"{m['p_profit'] * 100:>6.1f}% "
            f"{ci:>22}"
        )

    for b, m in zip(buckets, bucket_metrics):
        range_str = f"[{b['lo']:.0f}, {b['hi']:.0f}]"
        _row(b["label"], range_str, m)

    _row("ALL", "(all data)", all_metrics)


def print_comparison(
    cfg: dict,
    total_rows: int,
    markets_filter: Optional[list[str]],
    # First leg
    first_buckets: list[dict],
    first_metrics: list[dict],
    first_all: dict,
    first_null: int,
    first_summary: dict,
    # Second leg
    second_buckets: list[dict],
    second_metrics: list[dict],
    second_all: dict,
    second_null: int,
    second_summary: dict,
) -> None:
    markets_str = ", ".join(a.upper() for a in sorted(markets_filter)) if markets_filter else "all"
    sl = sl_str(cfg.get("stop_loss"))
    f_pct = f"{cfg['fraction'] * 100:.0f}%"

    print()
    print(f"Liquidity vs Outcomes  "
          f"(delta={cfg['delta']}, sl={sl}, "
          f"P1={cfg['p1_lo']}-{cfg['p1_hi']}, "
          f"time={cfg['time_lo']}-{cfg['time_hi']}, "
          f"f={f_pct}, markets={markets_str})")
    print("=" * 110)

    _print_leg_table("First-Leg", first_buckets, first_metrics, first_all, total_rows, first_null)
    _print_leg_table("Second-Leg", second_buckets, second_metrics, second_all, total_rows, second_null)

    # ── Comparison summary ───────────────────────────────────────────────────
    print()
    print("Comparison")
    print("=" * 70)

    for label, s, buckets in [
        ("First-leg", first_summary, first_buckets),
        ("Second-leg", second_summary, second_buckets),
    ]:
        best_b = buckets[s["best_idx"]]
        worst_b = buckets[s["worst_idx"]]
        mono_str = "YES" if s["monotonic"] else f"NO ({s['violations']} violations)"
        print(f"  {label:12s}  spread={s['spread']:.3f} ({s['signal']})  "
              f"monotonic={mono_str}  "
              f"best={best_b['label']}[{best_b['lo']:.0f}-{best_b['hi']:.0f}]  "
              f"worst={worst_b['label']}[{worst_b['lo']:.0f}-{worst_b['hi']:.0f}]")

    # Winner
    f_spread = first_summary["spread"]
    s_spread = second_summary["spread"]
    if f_spread > s_spread * 1.2:
        winner = "First-leg"
        ratio = f_spread / max(s_spread, 0.001)
    elif s_spread > f_spread * 1.2:
        winner = "Second-leg"
        ratio = s_spread / max(f_spread, 0.001)
    else:
        winner = None
        ratio = 0.0

    print()
    if winner:
        print(f"  >>> {winner} liquidity is the stronger predictor "
              f"(spread {ratio:.1f}x larger)")
        w_summary = first_summary if winner == "First-leg" else second_summary
        if w_summary["monotonic"]:
            print(f"      AND the relationship is monotonic — actionable signal.")
        else:
            print(f"      But NOT monotonic — pattern is noisy, use with caution.")
    else:
        print(f"  >>> Both legs show similar spread — no clear winner "
              f"(first={f_spread:.3f}, second={s_spread:.3f})")
    print()


# =============================================================================
# Main
# =============================================================================

async def run(
    db_url:          str,
    cfg:             dict,
    n_buckets:       int = 5,
    date_after:      Optional[str] = None,
    date_before:     Optional[str] = None,
    markets:         Optional[list[str]] = None,
    min_second_ask:  Optional[float] = None,
    max_second_ask:  Optional[float] = None,
    use_parquet:     bool = False,
) -> None:
    markets_str = ", ".join(a.upper() for a in sorted(markets)) if markets else "all"
    print(f"\nLiquidity Analysis  (markets={markets_str})")
    print(f"  Config: delta={cfg['delta']}, sl={sl_str(cfg.get('stop_loss'))}, "
          f"P1={cfg['p1_lo']}-{cfg['p1_hi']}, "
          f"time={cfg['time_lo']}-{cfg['time_hi']}, "
          f"f={cfg['fraction']:.0%}")
    if date_after:
        print(f"  After: {date_after}")
    if date_before:
        print(f"  Before: {date_before}")
    if min_second_ask is not None:
        print(f"  Min second-leg ask: {min_second_ask:.0f}")
    if max_second_ask is not None:
        print(f"  Max second-leg ask: {max_second_ask:.0f}")
    print()

    # Fetch data (single query — both legs come from the same rows)
    print("  Fetching attempts with liquidity data...")
    rows = await fetch_attempts_with_liquidity(
        db_url, cfg, date_after, date_before, markets, use_parquet=use_parquet,
    )
    total_rows = len(rows)
    print(f"  Got {total_rows} distinct markets.")

    # Apply second-leg ask filter if requested
    if min_second_ask is not None or max_second_ask is not None:
        before_filter = len(rows)
        filtered = []
        for r in rows:
            val = _extract_ask(r, "second")
            if val is None:
                continue  # drop nulls when filtering
            if min_second_ask is not None and val < min_second_ask:
                continue
            if max_second_ask is not None and val > max_second_ask:
                continue
            filtered.append(r)
        rows = filtered
        total_rows = len(rows)
        print(f"  After second-leg ask filter: {total_rows}/{before_filter} rows remain.")

    if total_rows == 0:
        print("  No data found for this config. Exiting.\n")
        return

    fraction = cfg["fraction"]
    results = {}

    for leg in ("first", "second"):
        with_data, null_count = split_by_liquidity(rows, leg)
        print(f"  {leg.capitalize()}-leg: {len(with_data)} rows with data, {null_count} nulls.")

        if len(with_data) < n_buckets:
            print(f"  Too few rows for {n_buckets} buckets. Skipping {leg}-leg.\n")
            results[leg] = None
            continue

        buckets = bucket_by_quantile(with_data, n_buckets)
        bucket_metrics = [analyze_bucket(b["rows"], fraction) for b in buckets]
        all_metrics = analyze_bucket(with_data, fraction)
        summary = _summarize(bucket_metrics, buckets)

        results[leg] = {
            "buckets": buckets,
            "metrics": bucket_metrics,
            "all": all_metrics,
            "null_count": null_count,
            "summary": summary,
        }

    first = results.get("first")
    second = results.get("second")

    if not first and not second:
        print("  Neither leg has enough data. Exiting.\n")
        return

    if first and second:
        print_comparison(
            cfg, total_rows, markets,
            first["buckets"], first["metrics"], first["all"], first["null_count"], first["summary"],
            second["buckets"], second["metrics"], second["all"], second["null_count"], second["summary"],
        )
    else:
        # Only one leg has data — print just that one
        leg_name = "First-Leg" if first else "Second-Leg"
        r = first or second
        print(f"\n  Only {leg_name} has sufficient data:")
        _print_leg_table(
            leg_name, r["buckets"], r["metrics"], r["all"], total_rows, r["null_count"],
        )
        s = r["summary"]
        best_b = r["buckets"][s["best_idx"]]
        mono_str = "YES" if s["monotonic"] else f"NO ({s['violations']} violations)"
        print(f"\n  Spread: {s['spread']:.3f} ({s['signal']})  Monotonic: {mono_str}  "
              f"Best: {best_b['label']}[{best_b['lo']:.0f}-{best_b['hi']:.0f}]\n")


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
        description="Explore whether liquidity at entry predicts pair rate and profitability"
    )
    parser.add_argument("--config", required=True,
                        help="Config string: delta=<int>,sl=<int>,p1_lo=<int>,p1_hi=<int>,"
                             "time_lo=<int>,time_hi=<int>[,f=<float>]")
    parser.add_argument("--markets", default=None,
                        help="Comma-separated crypto assets (default: all)")
    parser.add_argument("--buckets", type=int, default=5,
                        help="Number of liquidity quantile buckets (default: 5)")
    parser.add_argument("--after", default=None,
                        help="Only use data after this date (YYYY-MM-DD)")
    parser.add_argument("--before", default=None,
                        help="Only use data before this date (YYYY-MM-DD)")
    parser.add_argument("--min-second-ask", type=float, default=None,
                        help="Only include rows where second-leg best ask >= this value")
    parser.add_argument("--max-second-ask", type=float, default=None,
                        help="Only include rows where second-leg best ask <= this value")
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument("--use-parquet", action="store_true",
                        help="Query DuckDB/Parquet instead of Postgres "
                             "(requires: python scripts/export_to_parquet.py --days N first)")
    args = parser.parse_args()

    cfg = _parse_config_str(args.config)
    db_url = _resolve_db_url(args)
    markets = _resolve_markets(args)

    asyncio.run(run(
        db_url=db_url,
        cfg=cfg,
        n_buckets=args.buckets,
        date_after=args.after,
        date_before=args.before,
        markets=markets,
        min_second_ask=args.min_second_ask,
        max_second_ask=args.max_second_ask,
        use_parquet=args.use_parquet,
    ))


if __name__ == "__main__":
    main()
