#!/usr/bin/env python3
"""Walk-Forward Validation

Two modes:

  MODE A — Fixed-config rolling evaluation (use when you already have configs)
  ─────────────────────────────────────────────────────────────────────────────
  Pass one or more --config flags.  Each config is evaluated across every
  rolling window of --window-days length, stepping --step-days at a time.
  All data is used for evaluation — no training split needed.

  Format:  delta=<int>,sl=<int>,p1_lo=<int>,p1_hi=<int>,time_lo=<int>,time_hi=<int>[,f=<float>]
  f defaults to 0.15 if omitted.

  Examples:
    python scripts/walk_forward.py \\
      --config "delta=12,sl=31,p1_lo=52,p1_hi=60,time_lo=3,time_hi=4" \\
      --config "delta=10,sl=35,p1_lo=64,p1_hi=68,time_lo=2,time_hi=4" \\
      --window-days 4 --step-days 1 --markets BTC

    python scripts/walk_forward.py --configs-file my_configs.json --window-days 4

  JSON file format (array of objects with the same keys as the CLI format):
    [
      {"delta": 12, "sl": 31, "p1_lo": 52, "p1_hi": 60, "time_lo": 3, "time_hi": 4, "f": 0.15},
      {"delta": 10, "sl": 35, "p1_lo": 64, "p1_hi": 68, "time_lo": 2, "time_hi": 4}
    ]

  MODE B — IS/OOS optimizer validation (use to test whether the optimizer overfits)
  ──────────────────────────────────────────────────────────────────────────────────
  Run without --config / --configs-file.  For each fold the optimizer runs on
  the training window and the resulting best config is tested on the held-out
  test window.

  Examples:
    python scripts/walk_forward.py
    python scripts/walk_forward.py --train-days 8 --test-days 3 --step-days 1
    python scripts/walk_forward.py --top-k 3
    python scripts/walk_forward.py --markets BTC,ETH
    python scripts/walk_forward.py --min-profit 0.1 --min-width 5 --min-time 2
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
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

# ─── borrow pure-computation helpers from the optimizer ──────────────────────
_query                     = _opt._query
NET_PNL_EXPR               = _opt.NET_PNL_EXPR
_TAKER_FEE_SQL             = _opt._TAKER_FEE_SQL
REINVEST_FRACTIONS         = _opt.REINVEST_FRACTIONS
simulate_compound_bankroll = _opt.simulate_compound_bankroll
bootstrap_bankroll_stats   = _opt.bootstrap_bankroll_stats
_dedup_configs             = _opt._dedup_configs
_build_2d_arrays           = _opt._build_2d_arrays
_prefix_sum_2d             = _opt._prefix_sum_2d
_box_query_2d              = _opt._box_query_2d
sl_str                     = _opt.sl_str
time_range_str             = _opt.time_range_str


# =============================================================================
# WHERE / filter helpers — date-range-aware (adds date_before support)
# =============================================================================

def _base_where_range(
    date_after:  Optional[datetime],
    date_before: Optional[datetime],
    idx_start:   int = 1,
    markets:     Optional[list[str]] = None,
) -> tuple[str, list]:
    """Shared WHERE clause with optional lower AND upper timestamp bounds."""
    parts = [
        "status IN ('completed_paired', 'completed_failed')",
        "S0_points = 1",
        "time_remaining_at_start <= 900",
        "delta_points >= 10",
        "stop_loss_threshold_points >= 28",
        "(100 - P1_points) >= delta_points",
        "(stop_loss_threshold_points IS NULL OR P1_points >= stop_loss_threshold_points)",
    ]
    params: list = []
    idx = idx_start
    if date_after:
        parts.append(f"t1_timestamp >= ${idx}")
        params.append(date_after.isoformat() if isinstance(date_after, datetime) else date_after)
        idx += 1
    if date_before:
        parts.append(f"t1_timestamp < ${idx}")
        params.append(date_before.isoformat() if isinstance(date_before, datetime) else date_before)
        idx += 1
    if markets:
        parts.append(
            f"market_id IN (SELECT market_id FROM Markets "
            f"WHERE crypto_asset = ANY(${idx}))"
        )
        params.append(markets)
    return "WHERE " + " AND ".join(parts), params


def _cfg_filter_range(
    cfg:         dict,
    date_after:  Optional[datetime],
    date_before: Optional[datetime],
    markets:     Optional[list[str]] = None,
) -> tuple[str, list]:
    """WHERE + params for a specific config within a date window."""
    where, params = _base_where_range(date_after, date_before, markets=markets)
    idx = len(params) + 1

    parts = [f"delta_points = ${idx}"]
    params.append(cfg["delta"])
    idx += 1

    if cfg.get("stop_loss") is not None:
        parts.append(f"stop_loss_threshold_points = ${idx}")
        params.append(cfg["stop_loss"])
        idx += 1
    else:
        parts.append("stop_loss_threshold_points IS NULL")

    parts.append(f"P1_points BETWEEN ${idx} AND ${idx + 1}")
    params.append(cfg["p1_lo"])
    params.append(cfg["p1_hi"])
    idx += 2

    parts.append(
        f"CEIL(time_remaining_at_start / 60)::int BETWEEN ${idx} AND ${idx + 1}"
    )
    params.append(cfg["time_lo"])
    params.append(cfg["time_hi"])

    return f"{where} AND {' AND '.join(parts)}", params


# =============================================================================
# DB queries
# =============================================================================

async def get_data_date_range(
    db_url:  str,
    markets: Optional[list[str]] = None,
) -> tuple[datetime, datetime]:
    """Return (earliest, latest) t1_timestamp in completed Attempts."""
    where, params = _base_where_range(None, None, markets=markets)
    sql = f"""
        SELECT
            MIN(t1_timestamp)::timestamptz AS min_ts,
            MAX(t1_timestamp)::timestamptz AS max_ts
        FROM Attempts
        {where}
    """
    rows = await _query(db_url, sql, params)
    if not rows or rows[0]["min_ts"] is None:
        raise ValueError("No completed attempts found in the database.")
    min_ts: datetime = rows[0]["min_ts"]
    max_ts: datetime = rows[0]["max_ts"]
    if min_ts.tzinfo is None:
        min_ts = min_ts.replace(tzinfo=timezone.utc)
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    return min_ts, max_ts


async def fetch_grid_range(
    db_url:      str,
    date_after:  Optional[datetime],
    date_before: Optional[datetime],
    markets:     Optional[list[str]] = None,
) -> list[dict]:
    """Stage 1: 4D grid aggregated within a date window."""
    where, params = _base_where_range(date_after, date_before, markets=markets)
    sql = f"""
        SELECT
            delta_points,
            stop_loss_threshold_points,
            P1_points                                            AS p1_points,
            CEIL(time_remaining_at_start / 60)::int              AS time_minute,
            COUNT(*)                                             AS attempts,
            SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) AS pairs,
            SUM({NET_PNL_EXPR})                                  AS total_pnl,
            MIN(t1_timestamp::timestamp)                         AS min_ts,
            MAX(t1_timestamp::timestamp)                         AS max_ts
        FROM Attempts
        {where}
        GROUP BY 1, 2, 3, 4
    """
    return await _query(db_url, sql, params)


async def fetch_outcomes_range(
    db_url:      str,
    cfg:         dict,
    date_after:  Optional[datetime],
    date_before: Optional[datetime],
    markets:     Optional[list[str]] = None,
) -> list[dict]:
    """One outcome per distinct market (first attempt chronologically) in the window."""
    full_where, params = _cfg_filter_range(cfg, date_after, date_before, markets)
    sql = f"""
        SELECT DISTINCT ON (market_id)
            market_id,
            t1_timestamp,
            status,
            P1_points,
            delta_points,
            COALESCE(stop_loss_threshold_points, P1_points) AS loss_points,
            CASE
                WHEN status = 'completed_failed'
                     AND stop_loss_threshold_points IS NOT NULL
                THEN {_TAKER_FEE_SQL}
                ELSE 0
            END AS taker_fee_points
        FROM Attempts
        {full_where}
        ORDER BY market_id, t1_timestamp ASC
    """
    rows = await _query(db_url, sql, params)
    rows.sort(key=lambda r: r["t1_timestamp"])
    return rows


# =============================================================================
# Box search — mirrors optimize_params.search_boxes with min_box_days exposed
# =============================================================================

def _search_boxes_wf(
    grid:           list[dict],
    min_avg_pnl:    float = 0.1,
    min_p1_width:   int   = 5,
    min_time_width: int   = 2,
    min_attempts:   int   = 30,
    min_box_days:   float = 5.0,
) -> list[dict]:
    """Box search with configurable min_box_days, suitable for shorter windows."""
    combo_cells: dict[tuple, list[dict]] = defaultdict(list)
    for row in grid:
        key = (row["delta_points"], row["stop_loss_threshold_points"])
        combo_cells[key].append(row)

    all_p1   = sorted({int(r["p1_points"])   for r in grid})
    all_time = sorted({int(r["time_minute"]) for r in grid})
    p1_arr   = np.array(all_p1)
    time_arr = np.array(all_time)
    n_p1     = len(p1_arr)
    n_time   = len(time_arr)

    result: list[dict] = []

    for (delta, sl), cells in combo_cells.items():
        count_2d, pnl_2d, pairs_2d, min_ts_2d, max_ts_2d = _build_2d_arrays(
            cells, p1_arr, time_arr
        )
        if count_2d.sum() < min_attempts:
            continue

        ps_count = _prefix_sum_2d(count_2d)
        ps_pnl   = _prefix_sum_2d(pnl_2d)
        ps_pairs = _prefix_sum_2d(pairs_2d)

        for p1a in range(n_p1):
            for p1b in range(p1a + min_p1_width - 1, n_p1):
                if int(p1_arr[p1b]) - int(p1_arr[p1a]) + 1 < min_p1_width:
                    continue
                for ta in range(n_time):
                    for tb in range(ta + min_time_width - 1, n_time):
                        cnt = _box_query_2d(ps_count, p1a, p1b, ta, tb)
                        if cnt < min_attempts:
                            continue
                        pnl = _box_query_2d(ps_pnl, p1a, p1b, ta, tb)
                        if pnl / cnt < min_avg_pnl:
                            continue

                        box_slice = count_2d[p1a:p1b + 1, ta:tb + 1]
                        has_data  = box_slice > 0
                        if not has_data.any():
                            continue
                        box_min = float(min_ts_2d[p1a:p1b + 1, ta:tb + 1][has_data].min())
                        box_max = float(max_ts_2d[p1a:p1b + 1, ta:tb + 1][has_data].max())
                        box_hrs = (box_max - box_min) / 3600.0
                        if box_hrs <= 0 or box_hrs / 24.0 < min_box_days:
                            continue

                        pairs    = _box_query_2d(ps_pairs, p1a, p1b, ta, tb)
                        box_days = box_hrs / 24.0
                        result.append({
                            "delta":       int(delta) if delta is not None else 0,
                            "stop_loss":   int(sl)    if sl    is not None else None,
                            "p1_lo":       int(p1_arr[p1a]),
                            "p1_hi":       int(p1_arr[p1b]),
                            "time_lo":     int(time_arr[ta]),
                            "time_hi":     int(time_arr[tb]),
                            "attempts":    int(cnt),
                            "pairs":       int(pairs),
                            "pair_rate":   pairs / max(1, cnt),
                            "avg_pnl":     pnl / cnt,
                            "total_pnl":   pnl,
                            "box_days":    box_days,
                            "pnl_per_day": pnl / box_days,
                        })

    return result


# =============================================================================
# Net PNL from a market-outcome row
# =============================================================================

def _net_pnl(m: dict) -> float:
    if m["status"] == "completed_paired":
        return float(m["delta_points"])
    return -(float(m["loss_points"]) + float(m.get("taker_fee_points") or 0))


# =============================================================================
# Per-fold: in-sample optimization
# =============================================================================

async def optimize_window(
    db_url:         str,
    date_after:     datetime,
    date_before:    datetime,
    markets:        Optional[list[str]] = None,
    min_avg_pnl:    float = 0.1,
    min_p1_width:   int   = 5,
    min_time_width: int   = 2,
    top_k:          int   = 3,
    min_box_days:   float = 5.0,
) -> list[dict]:
    """Run Stages 1-3 on the training window; return top_k ranked configs."""
    grid = await fetch_grid_range(db_url, date_after, date_before, markets)
    if not grid:
        return []

    configs = _search_boxes_wf(
        grid,
        min_avg_pnl=min_avg_pnl,
        min_p1_width=min_p1_width,
        min_time_width=min_time_width,
        min_box_days=min_box_days,
    )
    if not configs:
        return []

    configs.sort(key=lambda c: c["attempts"], reverse=True)
    configs = _dedup_configs(configs)

    variants: list[dict] = []
    for cfg in configs:
        outcomes = await fetch_outcomes_range(db_url, cfg, date_after, date_before, markets)
        if not outcomes:
            continue
        base = dict(cfg)
        base["distinct_markets"] = len(outcomes)
        for fraction in REINVEST_FRACTIONS:
            v = dict(base)
            v["fraction"]       = fraction
            v["final_bankroll"] = simulate_compound_bankroll(outcomes, fraction)
            v["compound_return"] = (v["final_bankroll"] - 1) * 100
            bstats = bootstrap_bankroll_stats(outcomes, fraction)
            v.update(bstats)
            variants.append(v)

    variants.sort(key=lambda c: c.get("mean_log", -np.inf), reverse=True)
    return variants[:top_k]


# =============================================================================
# Per-fold: out-of-sample evaluation
# =============================================================================

async def evaluate_oos(
    db_url:      str,
    cfg:         dict,
    date_after:  datetime,
    date_before: datetime,
    markets:     Optional[list[str]] = None,
) -> dict:
    """Apply a fixed config to the test window and return performance metrics."""
    outcomes = await fetch_outcomes_range(db_url, cfg, date_after, date_before, markets)
    fraction = cfg["fraction"]

    if not outcomes:
        return {
            "distinct_markets": 0, "pairs": 0, "pair_rate": 0.0,
            "avg_pnl": 0.0, "total_pnl": 0.0,
            "final_bankroll": 1.0, "compound_return": 0.0,
            "ci_lo": 1.0, "ci_hi": 1.0, "median": 1.0,
            "p_profit": 0.0, "p_ruin_50": 0.0, "p_ruin_75": 0.0,
            "mean_log": 0.0, "no_data": True,
        }

    pnls        = [_net_pnl(m) for m in outcomes]
    pairs_count = sum(1 for m in outcomes if m["status"] == "completed_paired")
    total_pnl   = sum(pnls)
    broll       = simulate_compound_bankroll(outcomes, fraction)
    bstats      = bootstrap_bankroll_stats(outcomes, fraction)

    result: dict = {
        "distinct_markets": len(outcomes),
        "pairs":            pairs_count,
        "pair_rate":        pairs_count / len(outcomes),
        "avg_pnl":          total_pnl / len(outcomes),
        "total_pnl":        total_pnl,
        "final_bankroll":   broll,
        "compound_return":  (broll - 1) * 100,
        "no_data":          False,
    }
    result.update(bstats)
    return result


# =============================================================================
# Formatting helpers
# =============================================================================

def _fmt_date(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def _cfg_label(cfg: dict) -> str:
    """Human-readable config description."""
    return (
        f"delta={cfg['delta']}  SL={sl_str(cfg.get('stop_loss'))}"
        f"  f={cfg['fraction']:.0%}"
        f"  P1={cfg['p1_lo']}-{cfg['p1_hi']}c"
        f"  time={cfg['time_lo']}-{cfg['time_hi']}min"
    )


def _cfg_key(cfg: dict) -> str:
    """Short stable key for config-stability comparison."""
    return (
        f"d={cfg['delta']} SL={sl_str(cfg.get('stop_loss'))}"
        f" P1={cfg['p1_lo']}-{cfg['p1_hi']} t={cfg['time_lo']}-{cfg['time_hi']}"
    )


def _print_metrics_row(label: str, m: dict, col_w: int = 16) -> None:
    no_data = m.get("no_data", False)
    if no_data:
        print(f"  {label:<{col_w}}  (no matching attempts in this window)")
        return
    mkts   = m.get("distinct_markets", 0)
    pr     = m.get("pair_rate", 0.0) * 100
    apnl   = m.get("avg_pnl", 0.0)
    br     = m.get("final_bankroll", 1.0)
    elog   = m.get("mean_log", 0.0)
    pwin   = m.get("p_profit", 0.0) * 100
    ci_lo  = m.get("ci_lo", 1.0)
    ci_hi  = m.get("ci_hi", 1.0)
    print(
        f"  {label:<{col_w}}"
        f"  Mkts={mkts:>4}"
        f"  PairR={pr:>5.1f}%"
        f"  AvgPnL={apnl:>+6.2f}"
        f"  Bankroll={br:>6.3f}"
        f"  E[logB]={elog:>+7.3f}"
        f"  P(win)={pwin:>5.1f}%"
        f"  95% CI=[{ci_lo:.3f}, {ci_hi:.3f}]"
    )


# =============================================================================
# Main walk-forward loop
# =============================================================================

async def run_walk_forward(
    db_url:         str,
    train_days:     int   = 8,
    test_days:      int   = 3,
    step_days:      int   = 1,
    top_k:          int   = 1,
    markets:        Optional[list[str]] = None,
    min_avg_pnl:    float = 0.1,
    min_p1_width:   int   = 5,
    min_time_width: int   = 2,
) -> None:
    W = 90
    print(f"\n{'=' * W}")
    print(f"  WALK-FORWARD VALIDATION")
    print(
        f"  Train window: {train_days} days  |  Test window: {test_days} days"
        f"  |  Step: {step_days} day(s)  |  Top-k: {top_k}"
    )
    mkt_str = ", ".join(m.upper() for m in sorted(markets)) if markets else "all"
    print(f"  Markets: {mkt_str}")
    print(f"{'=' * W}\n")

    # ── Discover data range ───────────────────────────────────────────────────
    print("  Querying available data range…")
    data_start, data_end = await get_data_date_range(db_url, markets)
    total_days = (data_end - data_start).total_seconds() / 86400
    print(f"  Data: {_fmt_date(data_start)} → {_fmt_date(data_end)} ({total_days:.1f} days)\n")

    # ── Generate folds ────────────────────────────────────────────────────────
    # Each fold: train [fold_start, train_end)  →  test [train_end, test_end)
    # Require test window to be at least 80% covered by available data.
    folds: list[tuple[datetime, datetime, datetime, datetime]] = []
    fold_start = data_start
    while True:
        train_end = fold_start + timedelta(days=train_days)
        test_end  = train_end  + timedelta(days=test_days)
        min_coverage_end = train_end + timedelta(days=test_days * 0.8)
        if min_coverage_end > data_end:
            break
        folds.append((fold_start, train_end, train_end, test_end))
        fold_start += timedelta(days=step_days)

    if not folds:
        needed = train_days + test_days * 0.8
        print(f"  ERROR: Not enough data for a single fold.")
        print(f"  Need at least {needed:.0f} days, have {total_days:.1f}.")
        print(f"  Try reducing --train-days or --test-days.\n")
        return

    print(f"  Generated {len(folds)} fold(s).\n")

    # ─── min_box_days: require configs to span at least 60% of train window ──
    min_box_days = max(train_days * 0.60, 4.0)

    # ── Per-fold execution ────────────────────────────────────────────────────
    fold_results: list[dict] = []

    for fold_i, (train_start, train_end, test_start, test_end) in enumerate(folds, 1):
        print(f"{'─' * W}")
        print(
            f"  FOLD {fold_i}/{len(folds)}"
            f"  |  Train: {_fmt_date(train_start)} to {_fmt_date(train_end)}"
            f"  |  Test: {_fmt_date(test_start)} to {_fmt_date(min(test_end, data_end))}"
        )
        print(f"{'─' * W}")

        # IS: optimize on training window
        print(f"\n  [IS]  Running optimizer on train window…")
        is_configs = await optimize_window(
            db_url, train_start, train_end,
            markets=markets,
            min_avg_pnl=min_avg_pnl,
            min_p1_width=min_p1_width,
            min_time_width=min_time_width,
            top_k=top_k,
            min_box_days=min_box_days,
        )

        if not is_configs:
            print(f"  [IS]  No configs found in this training window — skipping fold.\n")
            fold_results.append({"fold": fold_i, "skipped": True})
            continue

        fold_pairs: list[dict] = []

        for rank, is_cfg in enumerate(is_configs, 1):
            print(f"\n  IS #{rank}:  {_cfg_label(is_cfg)}")
            _print_metrics_row("IN-SAMPLE", is_cfg)

            # OOS: apply fixed config to test window
            oos = await evaluate_oos(db_url, is_cfg, test_start, test_end, markets)
            _print_metrics_row("OUT-OF-SAMPLE", oos)

            # Inline decay comment
            if not oos.get("no_data"):
                is_log  = is_cfg.get("mean_log", 0.0)
                oos_log = oos.get("mean_log", 0.0)
                sign    = "POSITIVE" if oos_log > 0.05 else (
                          "NEAR-ZERO" if oos_log > -0.05 else "NEGATIVE")
                if is_log > 0.01:
                    pct = oos_log / is_log * 100
                    decay_str = f"  OOS retains {pct:.0f}% of IS E[logB]"
                else:
                    decay_str = ""
                print(f"  --> OOS E[logB] is {sign}.{decay_str}")

            fold_pairs.append({"is": is_cfg, "oos": oos})

        fold_results.append({
            "fold":    fold_i,
            "skipped": False,
            "train":   (train_start, train_end),
            "test":    (test_start, test_end),
            "configs": fold_pairs,
        })
        print()

    # ── Summary ───────────────────────────────────────────────────────────────
    _print_summary(fold_results, W)


# =============================================================================
# Summary report
# =============================================================================

def _print_summary(fold_results: list[dict], W: int = 90) -> None:
    print(f"\n{'=' * W}")
    print(f"  WALK-FORWARD SUMMARY  (top-1 IS config per fold)")
    print(f"{'=' * W}\n")

    valid = [f for f in fold_results if not f.get("skipped") and f.get("configs")]

    if not valid:
        print("  No valid folds completed — cannot produce a summary.\n")
        return

    is_logs:    list[float] = []
    oos_logs:   list[float] = []
    oos_brolls: list[float] = []
    cfg_keys:   list[str]   = []

    # ── Per-fold table ────────────────────────────────────────────────────────
    print(
        f"  {'Fold':<6}  {'IS Mkts':>7}  {'IS E[logB]':>10}  "
        f"{'OOS Mkts':>8}  {'OOS E[logB]':>11}  {'OOS Bankroll':>12}  Config"
    )
    print(f"  {'-'*6}  {'-'*7}  {'-'*10}  {'-'*8}  {'-'*11}  {'-'*12}  {'-'*30}")

    for fold in valid:
        top = fold["configs"][0]
        is_cfg  = top["is"]
        oos_res = top["oos"]

        is_log    = is_cfg.get("mean_log", 0.0)
        is_mkts   = is_cfg.get("distinct_markets", 0)
        cfg_key   = _cfg_key(is_cfg)

        is_logs.append(is_log)
        cfg_keys.append(cfg_key)

        if oos_res.get("no_data"):
            oos_log_s  = "  (no data)"
            oos_brol_s = "  (no data)"
            oos_mkts_s = "       -"
        else:
            oos_log    = oos_res.get("mean_log", 0.0)
            oos_broll  = oos_res.get("final_bankroll", 1.0)
            oos_mkts   = oos_res.get("distinct_markets", 0)
            oos_log_s  = f"{oos_log:>+11.3f}"
            oos_brol_s = f"{oos_broll:>12.3f}"
            oos_mkts_s = f"{oos_mkts:>8}"
            oos_logs.append(oos_log)
            oos_brolls.append(oos_broll)

        print(
            f"  {fold['fold']:<6}  {is_mkts:>7}  {is_log:>+10.3f}  "
            f"{oos_mkts_s}  {oos_log_s}  {oos_brol_s}  {cfg_key}"
        )

    # ── Aggregate statistics ──────────────────────────────────────────────────
    avg_is  = float(np.mean(is_logs))  if is_logs  else float("nan")
    avg_oos = float(np.mean(oos_logs)) if oos_logs else float("nan")

    print(f"\n  Avg IS  E[logB] across {len(is_logs)} fold(s):  {avg_is:>+.3f}")
    if not oos_logs:
        print(f"  Avg OOS E[logB]:  (no OOS data in any fold)")
    else:
        print(f"  Avg OOS E[logB] across {len(oos_logs)} fold(s):  {avg_oos:>+.3f}")
        if not np.isnan(avg_is) and avg_is > 0.01:
            retention = avg_oos / avg_is * 100
            print(f"  E[logB] retention (OOS / IS):  {retention:.1f}%")

    # ── Config stability ──────────────────────────────────────────────────────
    key_counts: dict[str, int] = {}
    for k in cfg_keys:
        key_counts[k] = key_counts.get(k, 0) + 1
    most_common_key   = max(key_counts, key=lambda k: key_counts[k])
    most_common_count = key_counts[most_common_key]
    print(
        f"\n  Config stability: {most_common_count}/{len(valid)} fold(s) chose "
        f"the same #1 config:"
    )
    print(f"    {most_common_key}")

    # ── Verdict ───────────────────────────────────────────────────────────────
    print(f"\n  VERDICT:")
    if np.isnan(avg_oos):
        print(f"  Cannot assess — no OOS data available.")
    elif avg_oos > 0.30:
        print(f"  ** STRONG EDGE **")
        print(f"     OOS E[logB] = {avg_oos:+.3f} is solidly positive.")
        print(f"     Parameters generalize well. The optimizer found a real edge.")
    elif avg_oos > 0.05:
        print(f"  ** MODERATE EDGE **")
        print(f"     OOS E[logB] = {avg_oos:+.3f} is positive but smaller than IS.")
        print(f"     Some IS inflation from the optimizer's search, but a genuine")
        print(f"     edge likely exists. The IS results are partially reliable.")
    elif avg_oos > -0.05:
        print(f"  ** UNCERTAIN **")
        print(f"     OOS E[logB] = {avg_oos:+.3f} is near zero.")
        print(f"     Edge may be real but too small to detect with limited OOS data,")
        print(f"     or parameters are moderately overfitted. Accumulate more data")
        print(f"     and re-run before deploying capital.")
    else:
        print(f"  ** OVERFITTING DETECTED **")
        print(f"     OOS E[logB] = {avg_oos:+.3f} is negative.")
        print(f"     The IS results do not generalize. Parameters are fitted to noise")
        print(f"     in the training window and are not reliable for live trading.")

    if most_common_count < len(valid):
        print(
            f"\n  NOTE: The optimizer chose different configs across folds "
            f"({len(key_counts)} distinct configs). Low stability suggests the "
            f"'optimal' parameters are sensitive to the exact data window used."
        )
    else:
        print(
            f"\n  NOTE: Config was stable — the same parameters won in every fold.")

    print(f"\n{'=' * W}\n")


# =============================================================================
# Fixed-config mode — parse, evaluate, report
# =============================================================================

def _parse_config_str(s: str) -> dict:
    """Parse a key=value comma-separated config string into a dict.

    Required keys: delta, sl, p1_lo, p1_hi, time_lo, time_hi
    Optional key:  f  (reinvest fraction, default 0.15)

    Example:
        "delta=12,sl=31,p1_lo=52,p1_hi=60,time_lo=3,time_hi=4,f=0.15"
    """
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


def _parse_configs_json(path: str) -> list[dict]:
    """Load configs from a JSON file (array of objects)."""
    import json
    with open(path) as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("configs JSON file must contain a top-level array")
    configs = []
    for item in data:
        configs.append({
            "delta":     int(item["delta"]),
            "stop_loss": int(item["sl"]),
            "p1_lo":     int(item["p1_lo"]),
            "p1_hi":     int(item["p1_hi"]),
            "time_lo":   int(item["time_lo"]),
            "time_hi":   int(item["time_hi"]),
            "fraction":  float(item.get("f", 0.15)),
        })
    return configs


async def run_fixed_config_test(
    db_url:      str,
    configs:     list[dict],
    window_days: int  = 4,
    step_days:   int  = 1,
    markets:     Optional[list[str]] = None,
) -> None:
    """Slide a rolling window over all data and evaluate each fixed config."""
    W = 100
    print(f"\n{'=' * W}")
    print(f"  FIXED-CONFIG ROLLING EVALUATION")
    print(
        f"  Window: {window_days} days  |  Step: {step_days} day(s)"
        f"  |  Configs: {len(configs)}"
    )
    mkt_str = ", ".join(m.upper() for m in sorted(markets)) if markets else "all"
    print(f"  Markets: {mkt_str}")
    print(f"{'=' * W}\n")

    # ── Data range ────────────────────────────────────────────────────────────
    print("  Querying available data range…")
    data_start, data_end = await get_data_date_range(db_url, markets)
    total_days = (data_end - data_start).total_seconds() / 86400
    print(f"  Data: {_fmt_date(data_start)} → {_fmt_date(data_end)} ({total_days:.1f} days)\n")

    # ── Generate windows ──────────────────────────────────────────────────────
    windows: list[tuple[datetime, datetime]] = []
    win_start = data_start
    while True:
        win_end = win_start + timedelta(days=window_days)
        if win_start + timedelta(days=window_days * 0.8) > data_end:
            break
        windows.append((win_start, win_end))
        win_start += timedelta(days=step_days)

    if not windows:
        needed = window_days * 0.8
        print(f"  ERROR: Not enough data for a single window.")
        print(f"  Need at least {needed:.0f} days, have {total_days:.1f}.")
        print(f"  Try reducing --window-days.\n")
        return

    print(f"  Generated {len(windows)} window(s) of {window_days} days each.\n")

    # ── Per-config evaluation ─────────────────────────────────────────────────
    all_cfg_summaries: list[dict] = []

    for cfg_i, cfg in enumerate(configs, 1):
        print(f"{'═' * W}")
        print(f"  CONFIG #{cfg_i}:  {_cfg_label(cfg)}")
        print(f"{'═' * W}\n")

        # Column header
        print(
            f"  {'Window':<24}  {'Mkts':>4}  {'PairR':>6}  {'AvgPnL':>7}  "
            f"{'Bankroll':>9}  {'E[logB]':>8}  {'P(win)':>6}  "
            f"{'95% CI':<21}"
        )
        print(f"  {'-'*24}  {'-'*4}  {'-'*6}  {'-'*7}  {'-'*9}  {'-'*8}  {'-'*6}  {'-'*21}")

        win_logs:    list[float] = []
        win_brolls:  list[float] = []
        win_pwins:   list[float] = []
        no_data_count = 0

        for win_start, win_end in windows:
            label = f"{_fmt_date(win_start)} → {_fmt_date(min(win_end, data_end))}"
            result = await evaluate_oos(db_url, cfg, win_start, win_end, markets)

            if result.get("no_data"):
                print(f"  {label:<24}  (no data)")
                no_data_count += 1
                continue

            mkts  = result["distinct_markets"]
            pr    = result["pair_rate"] * 100
            apnl  = result["avg_pnl"]
            br    = result["final_bankroll"]
            elog  = result["mean_log"]
            pwin  = result["p_profit"] * 100
            ci_lo = result["ci_lo"]
            ci_hi = result["ci_hi"]

            win_logs.append(elog)
            win_brolls.append(br)
            win_pwins.append(pwin)

            elog_flag = " ▲" if elog > 0.05 else (" ▼" if elog < -0.05 else "  ")
            print(
                f"  {label:<24}  {mkts:>4}  {pr:>5.1f}%  {apnl:>+7.2f}  "
                f"{br:>9.3f}  {elog:>+7.3f}{elog_flag}  {pwin:>5.1f}%  "
                f"[{ci_lo:.3f}, {ci_hi:.3f}]"
            )

        n_win = len(win_logs)
        if n_win == 0:
            print(f"\n  No data in any window for this config.\n")
            all_cfg_summaries.append({"cfg": cfg, "n_windows": 0})
            continue

        avg_elog  = float(np.mean(win_logs))
        avg_broll = float(np.mean(win_brolls))
        avg_pwin  = float(np.mean(win_pwins))
        pos_count = sum(1 for v in win_logs if v > 0.05)
        neg_count = sum(1 for v in win_logs if v < -0.05)
        neu_count = n_win - pos_count - neg_count

        print(f"\n  ── Summary for Config #{cfg_i} ──")
        print(f"  Windows with data: {n_win}  |  No data: {no_data_count}")
        print(
            f"  E[logB] distribution:  "
            f"positive={pos_count}  near-zero={neu_count}  negative={neg_count}"
        )
        print(
            f"  Avg E[logB]={avg_elog:>+.3f}  "
            f"Avg bankroll={avg_broll:.3f}  "
            f"Avg P(win)={avg_pwin:.1f}%"
        )

        # Per-config verdict
        if avg_elog > 0.30:
            verdict = "STRONG EDGE — consistently profitable across windows"
        elif avg_elog > 0.05:
            verdict = "MODERATE EDGE — mostly profitable, some window variance"
        elif avg_elog > -0.05:
            verdict = "UNCERTAIN — edge too small to confirm with available data"
        else:
            verdict = "NO EDGE — negative drift across windows on average"
        print(f"  Verdict: {verdict}\n")

        all_cfg_summaries.append({
            "cfg":       cfg,
            "n_windows": n_win,
            "avg_elog":  avg_elog,
            "avg_broll": avg_broll,
            "avg_pwin":  avg_pwin,
            "pos_count": pos_count,
            "neg_count": neg_count,
            "verdict":   verdict,
        })

    # ── Cross-config comparison table ─────────────────────────────────────────
    if len(configs) > 1:
        print(f"\n{'=' * W}")
        print(f"  CROSS-CONFIG COMPARISON  (ranked by Avg E[logB])")
        print(f"{'=' * W}\n")

        valid_sums = [s for s in all_cfg_summaries if s.get("n_windows", 0) > 0]
        valid_sums.sort(key=lambda s: s.get("avg_elog", -np.inf), reverse=True)

        print(
            f"  {'#':>3}  {'Avg E[logB]':>11}  {'Avg Bankroll':>12}  "
            f"{'Avg P(win)':>10}  {'Pos/Neu/Neg':>12}  Config"
        )
        print(f"  {'-'*3}  {'-'*11}  {'-'*12}  {'-'*10}  {'-'*12}  {'-'*40}")
        for rank, s in enumerate(valid_sums, 1):
            cfg = s["cfg"]
            pnn = f"{s['pos_count']}/{s['n_windows'] - s['pos_count'] - s['neg_count']}/{s['neg_count']}"
            print(
                f"  {rank:>3}  {s['avg_elog']:>+11.3f}  {s['avg_broll']:>12.3f}  "
                f"{s['avg_pwin']:>9.1f}%  {pnn:>12}  {_cfg_label(cfg)}"
            )
        print()

    print(f"{'=' * W}\n")


# =============================================================================
# CLI
# =============================================================================

def _resolve_db_url(args) -> str:
    if args.db_url:
        return args.db_url
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    print("ERROR: No database URL. Set DATABASE_URL or pass --db-url.", file=sys.stderr)
    sys.exit(1)


def _resolve_markets(args) -> Optional[list[str]]:
    raw = getattr(args, "markets", None) or os.environ.get("OPTIMIZE_MARKETS", "").strip()
    if not raw:
        return None
    assets = [a.strip().lower() for a in raw.split(",") if a.strip()]
    return assets or None


def main() -> None:
    load_env_file()
    parser = argparse.ArgumentParser(
        description=(
            "Walk-forward validation.  "
            "Pass --config / --configs-file to evaluate specific configs across rolling windows "
            "(Mode A).  Omit them to run IS/OOS optimizer validation (Mode B)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument(
        "--markets", default=None,
        help="Comma-separated crypto assets (e.g. BTC,SOL; default: all)",
    )

    # ── Mode A: fixed-config rolling evaluation ───────────────────────────────
    grp_a = parser.add_argument_group(
        "Mode A — fixed-config rolling evaluation",
        "Provide one or more configs; each is evaluated across rolling windows of --window-days.",
    )
    grp_a.add_argument(
        "--config", dest="configs", action="append", default=[],
        metavar="delta=N,sl=N,p1_lo=N,p1_hi=N,time_lo=N,time_hi=N[,f=0.15]",
        help="Config to test (repeatable).  f defaults to 0.15.",
    )
    grp_a.add_argument(
        "--configs-file", default=None,
        metavar="PATH",
        help="JSON file with an array of config objects (alternative to --config).",
    )
    grp_a.add_argument(
        "--window-days", type=int, default=4,
        help="Rolling window length in days for Mode A (default: 4)",
    )

    # ── Mode B: IS/OOS optimizer validation ──────────────────────────────────
    grp_b = parser.add_argument_group(
        "Mode B — IS/OOS optimizer validation",
        "Used when no --config flags are provided.",
    )
    grp_b.add_argument(
        "--train-days", type=int, default=8,
        help="Training window length in days (default: 8)",
    )
    grp_b.add_argument(
        "--test-days", type=int, default=3,
        help="Test window length in days (default: 3)",
    )
    grp_b.add_argument(
        "--top-k", type=int, default=1,
        help="Number of top IS configs to evaluate OOS per fold (default: 1)",
    )
    grp_b.add_argument(
        "--min-profit", type=float, default=0.1,
        help="Min avg PNL per attempt in pts (default: 0.1)",
    )
    grp_b.add_argument(
        "--min-width", type=int, default=5,
        help="Min P1 range width in cents (default: 5)",
    )
    grp_b.add_argument(
        "--min-time", type=int, default=2,
        help="Min time range width in minutes (default: 2)",
    )

    # ── Shared ────────────────────────────────────────────────────────────────
    parser.add_argument(
        "--step-days", type=int, default=1,
        help="Days to advance the window between folds/windows (default: 1)",
    )

    args    = parser.parse_args()
    db_url  = _resolve_db_url(args)
    markets = _resolve_markets(args)

    # ── Choose mode ───────────────────────────────────────────────────────────
    configs: list[dict] = []

    if args.configs_file:
        try:
            configs = _parse_configs_json(args.configs_file)
        except Exception as e:
            print(f"ERROR reading --configs-file: {e}", file=sys.stderr)
            sys.exit(1)

    for s in args.configs:
        try:
            configs.append(_parse_config_str(s))
        except ValueError as e:
            print(f"ERROR parsing --config {s!r}: {e}", file=sys.stderr)
            sys.exit(1)

    if configs:
        # Mode A: fixed-config rolling evaluation
        asyncio.run(run_fixed_config_test(
            db_url,
            configs=configs,
            window_days=args.window_days,
            step_days=args.step_days,
            markets=markets,
        ))
    else:
        # Mode B: IS/OOS optimizer walk-forward
        asyncio.run(run_walk_forward(
            db_url,
            train_days=args.train_days,
            test_days=args.test_days,
            step_days=args.step_days,
            top_k=args.top_k,
            markets=markets,
            min_avg_pnl=args.min_profit,
            min_p1_width=args.min_width,
            min_time_width=args.min_time,
        ))


if __name__ == "__main__":
    main()
