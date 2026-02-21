#!/usr/bin/env python3
"""Joint Parameter Optimizer — find the best (delta, stop_loss,
P1 range, time-remaining range) configuration.

Uses a 2D prefix-sum box search to evaluate all dimensions jointly,
avoiding the sequential-filtering pitfall where profitable niches are
missed because one marginal dimension looked bad on average.

Three-stage pipeline:
    Stage 1 — SQL: single query aggregating by
              (delta, stop_loss, P1_points, time_minute).
              Returns a fine-grained 4D grid.
    Stage 2 — Python: for each (delta, stop_loss) combo, build 2D prefix-sum
              arrays (P1 × time) and enumerate all valid boxes to
              find profitable parameter regions.  Each box query is O(1).
    Stage 3 — SQL: bootstrap CI on daily PNL for top configs, then rank.

Usage:
    python scripts/optimize_params.py                   # all data, S0=1
    python scripts/optimize_params.py --after 2026-02-10
    python scripts/optimize_params.py --top 10
    python scripts/optimize_params.py --min-profit 0.3  # min avg pnl per attempt
    python scripts/optimize_params.py --min-width 5     # min P1 range width in pts
    python scripts/optimize_params.py --min-time 2      # min time range in minutes
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time as _time
from collections import defaultdict
from typing import Optional

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.config import load_env_file  # noqa: E402

# ---------------------------------------------------------------------------
# Net PNL SQL expression — structural delta-based formula
#
#   - Completed pairs earn delta (the designed profit margin)
#   - Failures lose stop_loss_threshold (if set) or the full P1 cost
# ---------------------------------------------------------------------------

NET_PNL_EXPR = """
  CASE
    WHEN status = 'completed_paired' THEN delta_points
    WHEN status = 'completed_failed' THEN -COALESCE(stop_loss_threshold_points, P1_points)
    ELSE 0
  END
"""

# ---------------------------------------------------------------------------
# Shared base WHERE — always completed attempts, S0=1
# ---------------------------------------------------------------------------

def _base_where(date_after: Optional[str], idx_start: int = 1) -> tuple[str, list]:
    parts = [
        "status IN ('completed_paired', 'completed_failed')",
        "S0_points = 1",
        "time_remaining_at_start <= 900",
        "(100 - P1_points) >= delta_points",
        "(stop_loss_threshold_points IS NULL OR P1_points >= stop_loss_threshold_points)",
    ]
    params: list = []
    idx = idx_start
    if date_after:
        parts.append(f"t1_timestamp >= ${idx}")
        params.append(date_after)
    return "WHERE " + " AND ".join(parts), params

# ---------------------------------------------------------------------------
# DB helper with retry
# ---------------------------------------------------------------------------

async def _query(db_url: str, sql: str, params: list | None = None) -> list[dict]:
    import asyncpg
    params = params or []
    last_exc: BaseException | None = None
    for attempt in range(3):
        try:
            conn = await asyncpg.connect(db_url, statement_cache_size=0)
            try:
                rows = await conn.fetch(sql, *params)
                return [dict(r) for r in rows]
            finally:
                await conn.close()
        except (asyncpg.ConnectionDoesNotExistError,
                asyncpg.ConnectionFailureError,
                asyncpg.QueryCanceledError,
                OSError) as exc:
            last_exc = exc
            wait = 2.0 * (2 ** attempt)
            print(f"  [db] Attempt {attempt + 1}/3 failed ({type(exc).__name__}), "
                  f"retrying in {wait:.0f}s…")
            await asyncio.sleep(wait)
    raise last_exc  # type: ignore[misc]


# ===================================================================
# STAGE 1 — Fine-grained 4D grid (single SQL query)
# ===================================================================

async def fetch_grid(db_url: str, date_after: Optional[str]) -> list[dict]:
    """Return one row per (delta, SL, P1, time_minute) cell."""
    where, params = _base_where(date_after)

    sql = f"""
        SELECT
            delta_points,
            stop_loss_threshold_points,
            P1_points                                           AS p1_points,
            CEIL(time_remaining_at_start / 60)::int             AS time_minute,
            COUNT(*)                                            AS attempts,
            SUM(CASE WHEN status='completed_paired' THEN 1 ELSE 0 END) AS pairs,
            SUM({NET_PNL_EXPR})                                 AS total_pnl,
            MIN(t1_timestamp::timestamp)                        AS min_ts,
            MAX(t1_timestamp::timestamp)                        AS max_ts
        FROM Attempts
        {where}
        GROUP BY 1, 2, 3, 4
    """

    print("  [Stage 1] Fetching 4D grid (delta × SL × P1 × time)…")
    rows = await _query(db_url, sql, params)
    print(f"  [Stage 1] Got {len(rows):,} grid cells.\n")
    return rows


# ===================================================================
# STAGE 2 — 2D prefix-sum box search (pure Python/numpy)
# ===================================================================

def _build_2d_arrays(
    cells: list[dict],
    p1_vals: np.ndarray,
    time_vals: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Build count, pnl_sum, pairs, min_ts, max_ts 2D arrays from grid cells.

    Returns arrays of shape (len(p1_vals), len(time_vals)).
    min_ts/max_ts store epoch seconds (inf/-inf where no data).
    """
    p1_idx = {v: i for i, v in enumerate(p1_vals)}
    time_idx = {v: i for i, v in enumerate(time_vals)}

    shape = (len(p1_vals), len(time_vals))
    count_2d = np.zeros(shape, dtype=np.float64)
    pnl_2d = np.zeros(shape, dtype=np.float64)
    pairs_2d = np.zeros(shape, dtype=np.float64)
    min_ts_2d = np.full(shape, np.inf, dtype=np.float64)
    max_ts_2d = np.full(shape, -np.inf, dtype=np.float64)

    for c in cells:
        pi = p1_idx.get(int(c["p1_points"]))
        ti = time_idx.get(int(c["time_minute"]))
        if pi is None or ti is None:
            continue
        count_2d[pi, ti] += int(c["attempts"])
        pnl_2d[pi, ti] += float(c["total_pnl"] or 0)
        pairs_2d[pi, ti] += int(c["pairs"])

        if c["min_ts"] is not None:
            epoch = c["min_ts"].timestamp()
            if epoch < min_ts_2d[pi, ti]:
                min_ts_2d[pi, ti] = epoch
        if c["max_ts"] is not None:
            epoch = c["max_ts"].timestamp()
            if epoch > max_ts_2d[pi, ti]:
                max_ts_2d[pi, ti] = epoch

    return count_2d, pnl_2d, pairs_2d, min_ts_2d, max_ts_2d


def _prefix_sum_2d(arr: np.ndarray) -> np.ndarray:
    """Compute inclusive 2D prefix sum along both axes."""
    return arr.cumsum(axis=0).cumsum(axis=1)


def _box_query_2d(ps: np.ndarray, p1a: int, p1b: int, ta: int, tb: int) -> float:
    """O(1) 2D box sum query using inclusion-exclusion on prefix sums."""
    val = ps[p1b, tb]
    if p1a > 0:
        val -= ps[p1a - 1, tb]
    if ta > 0:
        val -= ps[p1b, ta - 1]
    if p1a > 0 and ta > 0:
        val += ps[p1a - 1, ta - 1]
    return float(val)


def search_boxes(
    grid: list[dict],
    min_avg_pnl: float = 0.3,
    min_p1_width: int = 5,
    min_time_width: int = 2,
    min_attempts: int = 50,
    top_per_combo: int = 50,
) -> list[dict]:
    """For each (delta, SL) combo, enumerate all valid 2D boxes and keep the best.

    A "box" is defined by (P1_lo..P1_hi, time_lo..time_hi).
    Ranked by expected PNL per hour = avg_pnl × attempts_per_hour.
    """
    combo_cells: dict[tuple, list[dict]] = defaultdict(list)
    for row in grid:
        key = (row["delta_points"], row["stop_loss_threshold_points"])
        combo_cells[key].append(row)

    all_p1 = sorted({int(r["p1_points"]) for r in grid})
    all_time = sorted({int(r["time_minute"]) for r in grid})

    p1_arr = np.array(all_p1)
    time_arr = np.array(all_time)

    n_p1 = len(p1_arr)
    n_time = len(time_arr)

    print(f"  Axes: {n_p1} P1 values, {n_time} time-minute values")

    all_configs: list[dict] = []
    combos_processed = 0
    total_combos = len(combo_cells)

    for (delta, sl), cells in combo_cells.items():
        combos_processed += 1
        print(f"  [Stage 2] Combo {combos_processed}/{total_combos}: "
              f"delta={delta}, SL={sl_str(sl)}")

        count_2d, pnl_2d, pairs_2d, min_ts_2d, max_ts_2d = _build_2d_arrays(
            cells, p1_arr, time_arr,
        )

        total = count_2d.sum()
        if total < min_attempts:
            print(f"  [Stage 2]   Skipped (only {int(total)} attempts)")
            continue

        ps_count = _prefix_sum_2d(count_2d)
        ps_pnl = _prefix_sum_2d(pnl_2d)
        ps_pairs = _prefix_sum_2d(pairs_2d)

        combo_top: list[tuple[float, dict]] = []

        for p1a in range(n_p1):
            if p1a > 0 and p1a % 10 == 0:
                print(f"  [Stage 2]   P1 loop: {p1a}/{n_p1} start indices done…")
            for p1b in range(p1a + min_p1_width - 1, n_p1):
                p1_lo_val = int(p1_arr[p1a])
                p1_hi_val = int(p1_arr[p1b])
                if p1_hi_val - p1_lo_val + 1 < min_p1_width:
                    continue

                for ta in range(n_time):
                    for tb in range(ta + min_time_width - 1, n_time):
                        cnt = _box_query_2d(ps_count, p1a, p1b, ta, tb)
                        if cnt < min_attempts:
                            continue

                        pnl = _box_query_2d(ps_pnl, p1a, p1b, ta, tb)
                        avg = pnl / cnt

                        if avg < min_avg_pnl:
                            continue

                        # Box runtime from raw min/max timestamp arrays
                        box_slice = count_2d[p1a:p1b+1, ta:tb+1]
                        has_data = box_slice > 0
                        if not has_data.any():
                            continue
                        box_min = float(min_ts_2d[p1a:p1b+1, ta:tb+1][has_data].min())
                        box_max = float(max_ts_2d[p1a:p1b+1, ta:tb+1][has_data].max())
                        box_hours = (box_max - box_min) / 3600.0
                        if box_hours <= 0:
                            continue

                        pairs = _box_query_2d(ps_pairs, p1a, p1b, ta, tb)
                        att_per_hour = cnt / box_hours
                        exp_pnl_per_hour = avg * att_per_hour

                        cfg = {
                            "delta": int(delta) if delta is not None else 0,
                            "stop_loss": int(sl) if sl is not None else None,
                            "p1_lo": p1_lo_val,
                            "p1_hi": p1_hi_val,
                            "time_lo": int(time_arr[ta]),
                            "time_hi": int(time_arr[tb]),
                            "attempts": int(cnt),
                            "pairs": int(pairs),
                            "pair_rate": pairs / max(1, cnt),
                            "avg_pnl": avg,
                            "total_pnl": pnl,
                            "box_hours": box_hours,
                            "att_per_hour": att_per_hour,
                            "exp_pnl_per_hour": exp_pnl_per_hour,
                        }

                        if len(combo_top) < top_per_combo:
                            combo_top.append((pnl, cfg))
                            if len(combo_top) == top_per_combo:
                                combo_top.sort(key=lambda x: x[0])
                        elif pnl > combo_top[0][0]:
                            combo_top[0] = (pnl, cfg)
                            combo_top.sort(key=lambda x: x[0])

        all_configs.extend(cfg for _, cfg in combo_top)
        print(f"  [Stage 2]   Combo done: {len(combo_top)} configs found "
              f"(total so far: {len(all_configs)})")

    return all_configs


def _dedup_configs(configs: list[dict], max_overlap: float = 0.5) -> list[dict]:
    """Remove configs that overlap too much with a higher-ranked one.

    Two configs overlap if they share the same (delta, SL) and the
    intersection of their P1 and time ranges each cover more than
    max_overlap of the smaller range on that axis.
    """
    def _axis_overlap(lo1: int, hi1: int, lo2: int, hi2: int) -> float:
        overlap = max(0, min(hi1, hi2) - max(lo1, lo2) + 1)
        smaller = min(hi1 - lo1 + 1, hi2 - lo2 + 1)
        return overlap / smaller if smaller > 0 else 0.0

    kept: list[dict] = []
    for cfg in configs:
        dominated = False
        for prev in kept:
            if cfg["delta"] != prev["delta"] or cfg["stop_loss"] != prev["stop_loss"]:
                continue
            p1_ov = _axis_overlap(cfg["p1_lo"], cfg["p1_hi"], prev["p1_lo"], prev["p1_hi"])
            t_ov = _axis_overlap(cfg["time_lo"], cfg["time_hi"], prev["time_lo"], prev["time_hi"])
            if p1_ov > max_overlap and t_ov > max_overlap:
                dominated = True
                break
        if not dominated:
            kept.append(cfg)
    return kept


# ===================================================================
# STAGE 3 — Bootstrap CI for top configs
# ===================================================================

def _cfg_filter(cfg: dict, date_after: Optional[str]) -> tuple[str, list]:
    """Build the WHERE clause + params for a specific config's filters."""
    where, params = _base_where(date_after)
    idx = len(params) + 1

    parts = []
    parts.append(f"delta_points = ${idx}")
    params.append(cfg["delta"])
    idx += 1

    if cfg["stop_loss"] is not None:
        parts.append(f"stop_loss_threshold_points = ${idx}")
        params.append(cfg["stop_loss"])
        idx += 1
    else:
        parts.append("stop_loss_threshold_points IS NULL")

    parts.append(f"P1_points BETWEEN ${idx} AND ${idx + 1}")
    params.append(cfg["p1_lo"])
    params.append(cfg["p1_hi"])
    idx += 2

    parts.append(f"CEIL(time_remaining_at_start / 60)::int BETWEEN ${idx} AND ${idx + 1}")
    params.append(cfg["time_lo"])
    params.append(cfg["time_hi"])

    combo_filter = " AND ".join(parts)
    return f"{where} AND {combo_filter}", params


async def fetch_config_market_outcomes(
    db_url: str,
    cfg: dict,
    date_after: Optional[str],
) -> list[dict]:
    """Return one outcome per distinct market (first attempt chronologically)."""
    full_where, params = _cfg_filter(cfg, date_after)
    sql = f"""
        SELECT DISTINCT ON (market_id)
            market_id,
            t1_timestamp,
            status,
            P1_points,
            delta_points,
            COALESCE(stop_loss_threshold_points, P1_points) AS loss_points
        FROM Attempts
        {full_where}
        ORDER BY market_id, t1_timestamp ASC
    """
    rows = await _query(db_url, sql, params)
    rows.sort(key=lambda r: r["t1_timestamp"])
    return rows


async def fetch_config_attempt_details(
    db_url: str,
    cfg: dict,
    date_after: Optional[str],
) -> tuple[np.ndarray, float | None]:
    """Return per-attempt PNL values and avg time-to-pair for a config."""
    full_where, params = _cfg_filter(cfg, date_after)

    sql = f"""
        SELECT
            {NET_PNL_EXPR} AS attempt_pnl,
            time_to_pair_seconds
        FROM Attempts
        {full_where}
    """

    rows = await _query(db_url, sql, params)
    if not rows:
        return np.array([]), None
    pnls = np.array([float(r["attempt_pnl"]) for r in rows])
    ttp_vals = [float(r["time_to_pair_seconds"]) for r in rows
                if r["time_to_pair_seconds"] is not None]
    avg_ttp = sum(ttp_vals) / len(ttp_vals) if ttp_vals else None
    return pnls, avg_ttp


# ---------------------------------------------------------------------------
# Compound bankroll simulation
# ---------------------------------------------------------------------------

def simulate_compound_bankroll(
    markets: list[dict],
    fraction: float = 0.20,
) -> float:
    """Replay one-entry-per-market with compounding and return final bankroll.

    Starting bankroll = 1.0.  For each market, commit `fraction` of current
    bankroll.  Return on committed capital = delta/P1 (win) or loss/P1 (loss).
    """
    bankroll = 1.0
    for m in markets:
        p1 = m["p1_points"]
        if m["status"] == "completed_paired":
            bankroll *= (1 + fraction * m["delta_points"] / p1)
        else:
            bankroll *= (1 - fraction * m["loss_points"] / p1)
    return bankroll


def bootstrap_bankroll_ci(
    markets: list[dict],
    fraction: float = 0.20,
    n_resamples: int = 5000,
    confidence: float = 0.95,
) -> tuple[float, float]:
    """95% CI on final bankroll via percentile bootstrap over market outcomes."""
    n = len(markets)
    if n < 2:
        b = simulate_compound_bankroll(markets, fraction)
        return (b, b)
    rng = np.random.default_rng(42)
    indices = rng.integers(0, n, size=(n_resamples, n))
    results = np.empty(n_resamples)
    for i in range(n_resamples):
        sample = [markets[j] for j in indices[i]]
        results[i] = simulate_compound_bankroll(sample, fraction)
    alpha = 1 - confidence
    return (
        float(np.percentile(results, 100 * alpha / 2)),
        float(np.percentile(results, 100 * (1 - alpha / 2))),
    )


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def hr(char: str = "-", width: int = 100) -> str:
    return char * width


def sl_str(val) -> str:
    return str(int(val)) if val is not None else "-"


def time_range_str(lo: int, hi: int) -> str:
    return f"{lo}-{hi} min"


# ---------------------------------------------------------------------------
# Main report
# ---------------------------------------------------------------------------

async def run(
    db_url: str,
    top_n: int = 10,
    date_after: Optional[str] = None,
    min_avg_pnl: float = 0.3,
    min_p1_width: int = 5,
    min_time_width: int = 2,
    fraction: float = 0.20,
) -> None:
    print(f"\n{'=' * 100}")
    print(f"  JOINT PARAMETER OPTIMIZER  (S0=1)")
    print(f"  Filters: min avg PNL >= {min_avg_pnl} pts, "
          f"min P1 width >= {min_p1_width} pts, "
          f"min time width >= {min_time_width} min")
    print(f"  Bankroll fraction per market: {fraction:.0%}")
    if date_after:
        print(f"  Data since: {date_after}")
    print(f"{'=' * 100}\n")

    # ==================================================================
    # STAGE 1: Fetch 4D grid
    # ==================================================================
    grid = await fetch_grid(db_url, date_after)
    if not grid:
        print("  No completed attempts found.\n")
        return

    total_att = sum(int(r["attempts"]) for r in grid)
    n_combos = len({(r["delta_points"], r["stop_loss_threshold_points"]) for r in grid})
    print(f"  {total_att:,} total attempts across {n_combos} (delta, SL) combos\n")

    # ==================================================================
    # STAGE 2: 2D prefix-sum box search
    # ==================================================================
    print(f"  Stage 2: searching all (P1 range × time range) boxes…")
    t0 = _time.monotonic()

    configs = search_boxes(
        grid,
        min_avg_pnl=min_avg_pnl,
        min_p1_width=min_p1_width,
        min_time_width=min_time_width,
        min_attempts=50,
        top_per_combo=top_n * 3,
    )

    elapsed = _time.monotonic() - t0
    print(f"\n  Box search complete: {len(configs)} configs found in {elapsed:.1f}s\n")

    if not configs:
        print("  No profitable configs found with the current thresholds.")
        print("  Try lowering --min-profit, --min-width, or --min-time.\n")
        print(f"{'=' * 100}")
        print("  Done.")
        print(f"{'=' * 100}\n")
        return

    # ==================================================================
    # STAGE 3: Compound bankroll simulation + bootstrap CI
    # ==================================================================
    configs.sort(key=lambda c: (c["avg_pnl"], c["total_pnl"]), reverse=True)
    configs = _dedup_configs(configs)
    print(f"  After dedup: {len(configs)} distinct configs")

    print(f"  Stage 3a: simulating compound bankroll for {len(configs)} configs "
          f"(f={fraction:.0%})…")
    for i, cfg in enumerate(configs):
        outcomes = await fetch_config_market_outcomes(db_url, cfg, date_after)
        cfg["distinct_markets"] = len(outcomes)
        cfg["final_bankroll"] = simulate_compound_bankroll(outcomes, fraction)
        cfg["compound_return"] = (cfg["final_bankroll"] - 1) * 100
        cfg["_outcomes"] = outcomes
        if (i + 1) % 25 == 0:
            print(f"  [progress] {i + 1}/{len(configs)} simulations done…")

    configs.sort(key=lambda c: c["final_bankroll"], reverse=True)
    top = configs[:top_n]

    print(f"  Stage 3b: computing 95% CIs for top {len(top)} configs…")
    for i, cfg in enumerate(top):
        ci_lo, ci_hi = bootstrap_bankroll_ci(cfg["_outcomes"], fraction)
        cfg["ci_lo"] = ci_lo
        cfg["ci_hi"] = ci_hi

        _, avg_ttp = await fetch_config_attempt_details(db_url, cfg, date_after)
        cfg["avg_ttp"] = avg_ttp
        if (i + 1) % 5 == 0:
            print(f"  [progress] {i + 1}/{len(top)} CIs computed…")

    for cfg in configs:
        cfg.pop("_outcomes", None)

    # ==================================================================
    # Print results
    # ==================================================================
    print(f"\n{'=' * 120}")
    print(f"  TOP {len(top)} JOINT CONFIGURATIONS "
          f"(ranked by compound bankroll, f={fraction:.0%})")
    print(f"{'=' * 120}\n")

    header = (
        f"  {'#':>3}  {'Delta':>5}  {'SL':>4}  {'P1 Range':>10}  "
        f"{'Time':>10}  "
        f"{'Mkts':>5}  {'PairR':>6}  {'AvgPnL':>7}  "
        f"{'Bankroll':>9}  {'Return':>8}  {'CI (bankroll)':>20}"
    )
    print(header)
    print(f"  {hr(width=118)}")

    for i, c in enumerate(top, 1):
        p1_range = f"{c['p1_lo']}-{c['p1_hi']}¢"
        t_range = time_range_str(c["time_lo"], c["time_hi"])
        ci = f"[{c.get('ci_lo', 1):>8.3f}, {c.get('ci_hi', 1):>8.3f}]"
        print(
            f"  {i:>3}  "
            f"{c['delta']:>5}  "
            f"{sl_str(c['stop_loss']):>4}  "
            f"{p1_range:>10}  "
            f"{t_range:>10}  "
            f"{c['distinct_markets']:>5}  "
            f"{c['pair_rate']*100:>5.1f}%  "
            f"{c['avg_pnl']:>7.2f}  "
            f"{c['final_bankroll']:>9.3f}  "
            f"{c['compound_return']:>+7.1f}%  "
            f"{ci}"
        )

    for i, c in enumerate(top[:5], 1):
        print(f"\n  --- #{i} Detail ---")
        print(f"  delta={c['delta']}  SL={sl_str(c['stop_loss'])}  "
              f"P1={c['p1_lo']}-{c['p1_hi']}¢  "
              f"time={c['time_lo']}-{c['time_hi']}min")
        print(f"  Attempts: {c['attempts']:,}  |  Pairs: {c['pairs']:,}  |  "
              f"Pair rate: {c['pair_rate']*100:.1f}%  |  "
              f"Distinct markets: {c['distinct_markets']}")
        print(f"  Avg PNL: {c['avg_pnl']:.2f} pts/attempt  |  "
              f"Total PNL: {c['total_pnl']:,.0f} pts")
        print(f"  Bankroll: {c['final_bankroll']:.4f}  |  "
              f"Return: {c['compound_return']:+.1f}%  |  "
              f"95% CI: [{c.get('ci_lo', 1):.3f}, {c.get('ci_hi', 1):.3f}]")
        avg_ttp = c.get("avg_ttp")
        ttp_str = f"{avg_ttp:.1f}s" if avg_ttp is not None else "n/a"
        print(f"  Avg time to pair: {ttp_str}")
        if c.get("ci_lo", 1) > 1:
            print(f"  ** CI lower bound > 1.0 — compounding is profitable **")
        else:
            print(f"  !! CI includes bankroll loss — may not compound reliably !!")

    print(f"\n{'=' * 120}")
    print("  Optimization complete.")
    print(f"{'=' * 120}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _resolve_db_url(args) -> str:
    if args.db_url:
        return args.db_url
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    print("ERROR: No database URL.  Set DATABASE_URL or pass --db-url.", file=sys.stderr)
    sys.exit(1)


def main():
    load_env_file()
    parser = argparse.ArgumentParser(
        description="Find the best joint (delta, SL, P1 range, time range) config"
    )
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument("--top", type=int, default=10, help="Top N configs to show (default: 10)")
    parser.add_argument("--after", default=None, help="Date filter (YYYY-MM-DD)")
    parser.add_argument("--min-profit", type=float, default=0.3,
                        help="Min avg PNL per attempt in pts (default: 0.3)")
    parser.add_argument("--min-width", type=int, default=5,
                        help="Min P1 range width in cents (default: 5)")
    parser.add_argument("--min-time", type=int, default=2,
                        help="Min time range width in minutes (default: 2)")
    parser.add_argument("--fraction", type=float, default=0.20,
                        help="Bankroll fraction per market entry (default: 0.20)")
    args = parser.parse_args()

    db_url = _resolve_db_url(args)
    asyncio.run(run(
        db_url,
        top_n=args.top,
        date_after=args.after,
        min_avg_pnl=args.min_profit,
        min_p1_width=args.min_width,
        min_time_width=args.min_time,
        fraction=args.fraction,
    ))


if __name__ == "__main__":
    main()
