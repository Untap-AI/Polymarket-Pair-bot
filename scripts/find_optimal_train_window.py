#!/usr/bin/env python3
"""Optimal Training Window Experiment

Finds how many days of training data produce the best out-of-sample (OOS)
performance for the parameter optimizer.  Tests multiple training-window
lengths, each evaluated across rolling train→test folds.

Results are saved incrementally to a CSV file (crash-safe) and a ranked
summary comparison table is printed at the end.

Usage:
    python scripts/find_optimal_train_window.py --markets BTC
    python scripts/find_optimal_train_window.py --markets BTC --fresh
    python scripts/find_optimal_train_window.py --summary-only
    python scripts/find_optimal_train_window.py --markets BTC,ETH,SOL,XRP
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import os
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np

# ─── path setup ──────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SCRIPTS = os.path.dirname(os.path.abspath(__file__))
for _p in (_ROOT, _SCRIPTS):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from src.config import load_env_file  # noqa: E402
import walk_forward as _wf            # noqa: E402

_fmt_date = _wf._fmt_date
sl_str = _wf.sl_str

# ─── Constants ───────────────────────────────────────────────────────────────

TRAIN_CANDIDATES = [3, 5, 7, 10, 14, 21]
MIN_FOLDS = 5

CSV_COLUMNS = [
    "run_timestamp", "train_days", "fold_index",
    "train_start", "train_end", "test_start", "test_end",
    "is_skipped", "error",
    "cfg_delta", "cfg_sl", "cfg_p1_lo", "cfg_p1_hi",
    "cfg_time_lo", "cfg_time_hi", "cfg_fraction",
    "is_mean_log", "is_pair_rate", "is_distinct_markets",
    "oos_mean_log", "oos_bankroll", "oos_pair_rate",
    "oos_avg_pnl", "oos_distinct_markets", "oos_no_data",
]


# ─── Fold optimizer (thin wrapper with configurable n_resamples) ─────────────

MARKETS_PER_DAY = 96

async def _optimize_fold(
    db_url: str,
    date_after: datetime,
    date_before: datetime,
    markets: Optional[list[str]] = None,
    top_k: int = 1,
    n_resamples: int = 500,
    min_box_days: float = 1.0,
    min_participation: float = 0.30,
) -> list[dict]:
    """Run Stages 1-3 on a training window; return top_k configs ranked by
    E[log bankroll].

    Mirrors walk_forward.optimize_window but accepts n_resamples for speed
    and filters by distinct market participation (30% of available markets)
    instead of raw attempt count.
    """
    train_days = (date_before - date_after).total_seconds() / 86400
    min_markets = int(train_days * MARKETS_PER_DAY * min_participation)

    grid = await _wf.fetch_grid_range(db_url, date_after, date_before, markets)
    if not grid:
        return []

    configs = _wf._search_boxes_wf(
        grid,
        min_avg_pnl=0.1,
        min_p1_width=5,
        min_time_width=2,
        min_attempts=3,
        min_box_days=min_box_days,
    )
    if not configs:
        return []

    configs.sort(key=lambda c: c["attempts"], reverse=True)
    configs = _wf._dedup_configs(configs)

    if len(configs) > 30:
        configs = configs[:30]

    variants: list[dict] = []
    for cfg in configs:
        outcomes = await _wf.fetch_outcomes_range(
            db_url, cfg, date_after, date_before, markets,
        )
        if len(outcomes) < min_markets:
            continue
        base = dict(cfg)
        base["distinct_markets"] = len(outcomes)
        for fraction in _wf.REINVEST_FRACTIONS:
            v = dict(base)
            v["fraction"] = fraction
            v["final_bankroll"] = _wf.simulate_compound_bankroll(outcomes, fraction)
            v["compound_return"] = (v["final_bankroll"] - 1) * 100
            bstats = _wf.bootstrap_bankroll_stats(
                outcomes, fraction, n_resamples=n_resamples,
            )
            v.update(bstats)
            variants.append(v)

    variants.sort(key=lambda c: c.get("mean_log", -np.inf), reverse=True)
    return variants[:top_k]


# ─── CSV helpers ─────────────────────────────────────────────────────────────

def _load_existing_csv(path: str) -> set[tuple[int, int]]:
    """Return set of (train_days, fold_index) already completed."""
    done: set[tuple[int, int]] = set()
    if not os.path.exists(path):
        return done
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                done.add((int(row["train_days"]), int(row["fold_index"])))
            except (ValueError, KeyError):
                continue
    return done


def _ensure_csv(path: str) -> None:
    """Create the CSV with header if it doesn't exist."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()


def _append_row(path: str, row: dict) -> None:
    """Append a single row to the CSV."""
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow({k: row.get(k, "") for k in CSV_COLUMNS})


# ─── Summary table ───────────────────────────────────────────────────────────

def _print_summary(
    csv_path: str,
    test_days: int,
    data_start_str: str,
    data_end_str: str,
    total_days: float,
) -> None:
    """Load CSV and print the ranked summary comparison table."""
    if not os.path.exists(csv_path):
        print("\n  No results CSV found.\n")
        return

    rows: list[dict] = []
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            rows.append(row)
    if not rows:
        print("\n  CSV is empty — no folds completed.\n")
        return

    by_td: dict[int, list[dict]] = defaultdict(list)
    for r in rows:
        try:
            by_td[int(r["train_days"])].append(r)
        except (ValueError, KeyError):
            continue

    summaries: list[dict] = []
    for td in sorted(by_td.keys()):
        folds = by_td[td]
        n_folds = len(folds)
        n_skipped = sum(1 for f in folds if f.get("is_skipped") == "True")
        n_no_data = sum(
            1 for f in folds
            if f.get("oos_no_data") == "True" and f.get("is_skipped") != "True"
        )

        valid_oos: list[float] = []
        valid_is: list[float] = []
        profitable = 0

        for f in folds:
            if f.get("is_skipped") == "True" or f.get("oos_no_data") == "True":
                continue
            try:
                oos_ml = float(f["oos_mean_log"])
                valid_oos.append(oos_ml)
                if oos_ml > 0:
                    profitable += 1
            except (ValueError, KeyError):
                continue
            try:
                valid_is.append(float(f["is_mean_log"]))
            except (ValueError, KeyError):
                pass

        n_valid = len(valid_oos)
        if n_valid == 0:
            summaries.append({
                "td": td, "n_folds": n_folds, "n_valid": 0,
                "n_skipped": n_skipped, "n_no_data": n_no_data,
                "avg_oos": float("-inf"), "med_oos": float("-inf"),
                "pct_prof": 0.0, "avg_is": 0.0, "retention": 0.0,
            })
            continue

        avg_oos = float(np.mean(valid_oos))
        med_oos = float(np.median(valid_oos))
        pct_prof = profitable / n_valid * 100
        avg_is = float(np.mean(valid_is)) if valid_is else 0.0
        retention = (avg_oos / avg_is * 100) if avg_is > 0.01 else 0.0

        summaries.append({
            "td": td, "n_folds": n_folds, "n_valid": n_valid,
            "n_skipped": n_skipped, "n_no_data": n_no_data,
            "avg_oos": avg_oos, "med_oos": med_oos,
            "pct_prof": pct_prof, "avg_is": avg_is, "retention": retention,
        })

    summaries.sort(key=lambda s: s["avg_oos"], reverse=True)

    best_oos = summaries[0]["avg_oos"] if summaries else float("-inf")
    plateau_threshold = best_oos * 0.85 if best_oos > 0 else float("-inf")

    W = 100
    print(f"\n{'=' * W}")
    print(f"  OPTIMAL TRAINING WINDOW EXPERIMENT — SUMMARY")
    print(f"  Test window: {test_days} days fixed")
    print(f"  Data: {data_start_str} → {data_end_str} ({total_days:.0f} days)")
    print(f"{'=' * W}\n")

    print(
        f"  {'Train':>5}  {'Folds':>5}  {'Valid':>5}  {'Skip':>4}  "
        f"{'Avg OOS':>9}  {'Med OOS':>8}  {'Profitable':>10}  "
        f"{'IS→OOS':>8}  {'Flag':>7}"
    )
    print(
        f"  {'─' * 5}  {'─' * 5}  {'─' * 5}  {'─' * 4}  "
        f"{'─' * 9}  {'─' * 8}  {'─' * 10}  {'─' * 8}  {'─' * 7}"
    )

    best_td: int | None = None
    plateau_tds: list[int] = []

    for i, s in enumerate(summaries):
        td_label = f"{s['td']}d"

        if s["n_valid"] == 0:
            print(
                f"  {td_label:>5}  {s['n_folds']:>5}  {s['n_valid']:>5}  "
                f"{s['n_skipped']:>4}  {'n/a':>9}  {'n/a':>8}  "
                f"{'n/a':>10}  {'n/a':>8}  "
            )
            continue

        if i == 0:
            flag = "BEST"
            best_td = s["td"]
        elif s["avg_oos"] >= plateau_threshold:
            flag = "PLATEAU"
            plateau_tds.append(s["td"])
        else:
            flag = ""

        ret_str = f"{s['retention']:>7.1f}%" if s["retention"] > 0 else f"{'n/a':>8}"

        print(
            f"  {td_label:>5}  {s['n_folds']:>5}  {s['n_valid']:>5}  "
            f"{s['n_skipped']:>4}  {s['avg_oos']:>+9.3f}  {s['med_oos']:>+8.3f}  "
            f"{s['pct_prof']:>9.1f}%  {ret_str}  {flag:>7}"
        )

    print()
    if best_td is not None:
        all_viable = [f"{best_td}d"] + [f"{td}d" for td in plateau_tds]
        print(f"  RECOMMENDATION: {best_td}-day training window")
        if plateau_tds:
            print(
                f"  ({', '.join(all_viable)} are all in the plateau — all viable.)"
            )
    else:
        print(f"  RECOMMENDATION: Insufficient valid OOS data to rank.")

    print(f"\n{'=' * W}\n")


# ─── Main experiment loop ────────────────────────────────────────────────────

async def run_experiment(
    db_url: str,
    markets: Optional[list[str]],
    test_days: int,
    step_days: int,
    top_k: int,
    n_resamples: int,
    output_path: str,
    fresh: bool,
    summary_only: bool,
) -> None:
    W = 100
    mkt_str = (
        ", ".join(m.upper() for m in sorted(markets)) if markets else "all"
    )
    print(f"\n{'=' * W}")
    print(f"  OPTIMAL TRAINING WINDOW EXPERIMENT")
    print(
        f"  Markets: {mkt_str}  |  Test: {test_days}d  |  Step: {step_days}d  "
        f"  |  Top-k: {top_k}  |  Resamples: {n_resamples}"
    )
    print(f"{'=' * W}\n")

    # ── Step 1: Query data range ─────────────────────────────────────────────
    print("  Querying available data range...")
    data_start, data_end = await _wf.get_data_date_range(db_url, markets)
    total_days = (data_end - data_start).total_seconds() / 86400
    ds = _fmt_date(data_start)
    de = _fmt_date(data_end)
    print(f"  Data available: {ds} → {de} ({total_days:.0f} days)\n")

    if summary_only:
        _print_summary(output_path, test_days, ds, de, total_days)
        return

    # ── Step 2: Filter training-window candidates ────────────────────────────
    max_allowed = total_days * 0.8
    valid_candidates: list[int] = []
    for td in TRAIN_CANDIDATES:
        if td + test_days > max_allowed:
            print(
                f"  Dropping {td}d: train+test={td + test_days}d "
                f"> {max_allowed:.0f}d (80% of {total_days:.0f}d)"
            )
        else:
            valid_candidates.append(td)

    print(f"\n  Training window candidates: {valid_candidates}\n")

    # ── Step 3: Generate folds per candidate ─────────────────────────────────
    if fresh and os.path.exists(output_path):
        os.remove(output_path)
    _ensure_csv(output_path)
    existing = set() if fresh else _load_existing_csv(output_path)
    if existing:
        print(f"  Resuming: {len(existing)} existing fold results found in CSV.\n")

    Fold = tuple[int, datetime, datetime, datetime, datetime]
    plan: list[tuple[int, list[Fold], int]] = []
    total_to_run = 0

    for td in valid_candidates:
        folds: list[Fold] = []
        fold_start = data_start
        fold_idx = 0
        total_possible = 0
        while True:
            train_end = fold_start + timedelta(days=td)
            test_end = train_end + timedelta(days=test_days)
            if test_end > data_end:
                break
            fold_idx += 1
            total_possible += 1
            if (td, fold_idx) not in existing:
                folds.append((fold_idx, fold_start, train_end, train_end, test_end))
            fold_start += timedelta(days=step_days)

        if total_possible < MIN_FOLDS:
            print(
                f"  train={td}d: only {total_possible} fold(s) possible "
                f"(need >= {MIN_FOLDS}) — skipping."
            )
            continue

        remaining = len(folds)
        done = total_possible - remaining
        if remaining == 0:
            print(f"  train={td}d: all {total_possible} folds already complete.")
        else:
            print(
                f"  train={td}d: {remaining} folds to run "
                f"({done} already done, {total_possible} total)"
            )
        total_to_run += remaining
        plan.append((td, folds, total_possible))

    print(f"\n  Total folds to run: {total_to_run}\n")

    if total_to_run == 0:
        _print_summary(output_path, test_days, ds, de, total_days)
        return

    # ── Steps 4-6: Run folds and write CSV incrementally ─────────────────────
    folds_done = 0

    for td, folds, _n_total in plan:
        if not folds:
            continue

        min_box_days = max(td * 0.5, 1.0)

        for fold_idx, train_start, train_end, test_start, test_end in folds:
            folds_done += 1
            ts = _fmt_date(train_start)
            te = _fmt_date(train_end)
            os_ = _fmt_date(test_start)
            oe = _fmt_date(test_end)

            print(
                f"[train={td}d fold={fold_idx} | {folds_done}/{total_to_run}] "
                f"Train: {ts}→{te} | Test: {os_}→{oe}"
            )

            row: dict = {
                "run_timestamp": datetime.now(timezone.utc).isoformat(),
                "train_days": td,
                "fold_index": fold_idx,
                "train_start": ts,
                "train_end": te,
                "test_start": os_,
                "test_end": oe,
                "is_skipped": False,
                "error": "",
            }

            try:
                is_configs = await _optimize_fold(
                    db_url, train_start, train_end,
                    markets=markets,
                    top_k=top_k,
                    n_resamples=n_resamples,
                    min_box_days=min_box_days,
                )

                if not is_configs:
                    print(f"  → No config found (skipped)")
                    row["is_skipped"] = True
                    _append_row(output_path, row)
                    continue

                cfg = is_configs[0]
                row.update({
                    "cfg_delta": cfg["delta"],
                    "cfg_sl": cfg.get("stop_loss"),
                    "cfg_p1_lo": cfg["p1_lo"],
                    "cfg_p1_hi": cfg["p1_hi"],
                    "cfg_time_lo": cfg["time_lo"],
                    "cfg_time_hi": cfg["time_hi"],
                    "cfg_fraction": cfg["fraction"],
                    "is_mean_log": f"{cfg.get('mean_log', 0):.6f}",
                    "is_pair_rate": f"{cfg.get('pair_rate', 0):.6f}",
                    "is_distinct_markets": cfg.get("distinct_markets", 0),
                })

                print(
                    f"  IS: delta={cfg['delta']} "
                    f"SL={sl_str(cfg.get('stop_loss'))} "
                    f"P1={cfg['p1_lo']}-{cfg['p1_hi']}¢ "
                    f"time={cfg['time_lo']}-{cfg['time_hi']}min  "
                    f"E[logB]={cfg.get('mean_log', 0):+.3f}"
                )

                oos = await _wf.evaluate_oos(
                    db_url, cfg, test_start, test_end, markets,
                )

                row.update({
                    "oos_mean_log": f"{oos.get('mean_log', 0):.6f}",
                    "oos_bankroll": f"{oos.get('final_bankroll', 1):.6f}",
                    "oos_pair_rate": f"{oos.get('pair_rate', 0):.6f}",
                    "oos_avg_pnl": f"{oos.get('avg_pnl', 0):.6f}",
                    "oos_distinct_markets": oos.get("distinct_markets", 0),
                    "oos_no_data": oos.get("no_data", False),
                })

                if oos.get("no_data"):
                    print(f"  OOS: no matching attempts")
                else:
                    print(
                        f"  OOS: E[logB]={oos.get('mean_log', 0):+.3f}  "
                        f"Bankroll={oos.get('final_bankroll', 1):.3f}  "
                        f"PairR={oos.get('pair_rate', 0) * 100:.1f}%"
                    )

            except Exception:
                tb = traceback.format_exc()
                short = tb.strip().splitlines()[-1][:200]
                print(f"  → ERROR: {short}")
                row["is_skipped"] = True
                row["error"] = short

            _append_row(output_path, row)

    # ── Step 7: Summary ──────────────────────────────────────────────────────
    _print_summary(output_path, test_days, ds, de, total_days)


# ─── CLI ─────────────────────────────────────────────────────────────────────

def _resolve_db_url(args) -> str:
    if args.db_url:
        return args.db_url
    url = (
        os.environ.get("DATABASE_URL_SESSION")
        or os.environ.get("DATABASE_URL")
    )
    if url:
        return url
    print(
        "ERROR: No database URL. Set DATABASE_URL or pass --db-url.",
        file=sys.stderr,
    )
    sys.exit(1)


def _resolve_markets(args) -> Optional[list[str]]:
    raw = (
        getattr(args, "markets", None)
        or os.environ.get("OPTIMIZE_MARKETS", "").strip()
    )
    if not raw:
        return None
    assets = [a.strip().lower() for a in raw.split(",") if a.strip()]
    return assets or None


def main() -> None:
    load_env_file()
    parser = argparse.ArgumentParser(
        description=(
            "Find the optimal training window length for the parameter optimizer. "
            "Tests multiple window sizes and compares OOS performance."
        ),
    )
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument(
        "--markets", default=None,
        help="Comma-separated crypto assets, e.g. BTC,SOL "
             "(default: all; respects OPTIMIZE_MARKETS env var)",
    )
    parser.add_argument(
        "--test-days", type=int, default=3,
        help="OOS window length in days (default: 3)",
    )
    parser.add_argument(
        "--step-days", type=int, default=3,
        help="Days to advance between folds (default: 3)",
    )
    parser.add_argument(
        "--top-k", type=int, default=1,
        help="Top N configs from optimizer to test OOS per fold (default: 1)",
    )
    parser.add_argument(
        "--n-resamples", type=int, default=500,
        help="Bootstrap resamples for IS ranking (default: 500)",
    )
    parser.add_argument(
        "--output", default=os.path.join("data", "train_window_experiment.csv"),
        help="CSV output path (default: data/train_window_experiment.csv)",
    )
    parser.add_argument(
        "--fresh", action="store_true",
        help="Ignore existing CSV and re-run everything from scratch",
    )
    parser.add_argument(
        "--summary-only", action="store_true",
        help="Skip running folds; just print summary from existing CSV",
    )

    args = parser.parse_args()
    db_url = _resolve_db_url(args)
    markets = _resolve_markets(args)

    asyncio.run(run_experiment(
        db_url=db_url,
        markets=markets,
        test_days=args.test_days,
        step_days=args.step_days,
        top_k=args.top_k,
        n_resamples=args.n_resamples,
        output_path=args.output,
        fresh=args.fresh,
        summary_only=args.summary_only,
    ))


if __name__ == "__main__":
    main()
