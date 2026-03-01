#!/usr/bin/env python3
"""Daily parameter optimization report — runs optimize_params and sends via SendGrid.

Composable wrapper for optimize_params.py. Runs the full optimization pipeline,
then sends the results by email when SENDGRID_* env vars are set.

Usage:
    python scripts/daily_report.py
    python scripts/daily_report.py --after 2026-02-01
    python scripts/daily_report.py --top 5 --days 14

Schedule with cron (e.g. 8am daily):
    0 8 * * * cd /path/to/Polymarket-Pair-bot && .venv/bin/python scripts/daily_report.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timedelta

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _project_root)
sys.path.insert(0, os.path.join(_project_root, "scripts"))
from src.config import load_env_file  # noqa: E402

from optimize_params import _send_email_if_configured, run  # noqa: E402


def _resolve_db_url(args: argparse.Namespace) -> str:
    if getattr(args, "db_url", None):
        return args.db_url
    url = os.environ.get("DATABASE_URL")
    if url:
        return url
    print("ERROR: No database URL.  Set DATABASE_URL or pass --db-url.", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(
        description="Run parameter optimization and send daily report via email"
    )
    parser.add_argument("--db-url", default=None, help="PostgreSQL URL")
    parser.add_argument("--top", type=int, default=10, help="Top N configs (default: 10)")
    parser.add_argument(
        "--after",
        default=None,
        help="Date filter YYYY-MM-DD (default: --days ago)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Use data from last N days when --after not set (default: 30)",
    )
    parser.add_argument("--min-profit", type=float, default=0.3)
    parser.add_argument("--min-width", type=int, default=5)
    parser.add_argument("--min-time", type=int, default=2)
    args = parser.parse_args()

    date_after = args.after
    if not date_after:
        date_after = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    db_url = _resolve_db_url(args)

    subject, body = asyncio.run(run(
        db_url,
        top_n=args.top,
        date_after=date_after,
        min_avg_pnl=args.min_profit,
        min_p1_width=args.min_width,
        min_time_width=args.min_time,
    ))

    # Add date to subject for daily report clarity
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"{subject} — {today}"

    _send_email_if_configured(subject, body)


if __name__ == "__main__":
    main()
