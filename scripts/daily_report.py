#!/usr/bin/env python3
"""Daily parameter optimization report — runs optimize_params, walk-forward, and sends via SendGrid.

Composable wrapper for optimize_params.py. Runs the full optimization pipeline,
then runs walk-forward validation on the top configs, and sends combined results
by email when SENDGRID_* env vars are set.

Usage:
    python scripts/daily_report.py
    python scripts/daily_report.py --after 2026-02-01
    python scripts/daily_report.py --top 5 --days 14
    python scripts/daily_report.py --wf-window-days 7

Schedule with cron (e.g. 8am daily):
    0 8 * * * cd /path/to/Polymarket-Pair-bot && .venv/bin/python scripts/daily_report.py
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import os
import sys
from datetime import datetime, timedelta

_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _project_root)
sys.path.insert(0, os.path.join(_project_root, "scripts"))
from src.config import load_env_file  # noqa: E402

from optimize_params import run  # noqa: E402
import walk_forward as _wf  # noqa: E402


class _Tee:
    def __init__(self, *files): self.files = files
    def write(self, data): [f.write(data) for f in self.files]
    def flush(self): [f.flush() for f in self.files]


def _send_email_if_configured(subject: str, body: str) -> None:
    """Send email via SendGrid if SENDGRID_API_KEY is set. Logs warning on failure."""
    api_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    if not api_key:
        return

    from_email = os.environ.get("SENDGRID_FROM_EMAIL", "").strip()
    to_email = os.environ.get("SENDGRID_TO_EMAIL", "").strip()
    if not from_email or not to_email:
        print("  [email] SENDGRID_FROM_EMAIL and SENDGRID_TO_EMAIL required; skipping send.",
              file=sys.stderr)
        return

    try:
        from sendgrid import SendGridAPIClient
        from sendgrid.helpers.mail import Content, Email, Mail, To

        message = Mail(
            from_email=Email(from_email),
            to_emails=To(to_email),
            subject=subject,
            plain_text_content=Content("text/plain", body),
        )
        sg = SendGridAPIClient(api_key=api_key)
        sg.send(message)
        print(f"  [email] Sent to {to_email}")
    except Exception as e:
        print(f"  [email] Send failed: {e}", file=sys.stderr)


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
    parser.add_argument(
        "--wf-window-days",
        type=int,
        default=4,
        help="Walk-forward rolling window size in days (default: 4)",
    )
    parser.add_argument(
        "--markets",
        default=None,
        help="Comma-separated crypto assets to include, e.g. BTC,SOL,ETH",
    )
    args = parser.parse_args()

    date_after = args.after
    if not date_after:
        date_after = (datetime.now() - timedelta(days=args.days)).strftime("%Y-%m-%d")

    db_url = _resolve_db_url(args)
    markets = None
    if args.markets:
        markets = [a.strip().lower() for a in args.markets.split(",") if a.strip()] or None

    subject, body, top_configs = asyncio.run(run(
        db_url,
        top_n=args.top,
        date_after=date_after,
        min_avg_pnl=args.min_profit,
        min_p1_width=args.min_width,
        min_time_width=args.min_time,
        markets=markets,
    ))

    if top_configs:
        buf = io.StringIO()
        with contextlib.redirect_stdout(_Tee(sys.stdout, buf)):
            asyncio.run(_wf.run_fixed_config_test(
                db_url,
                configs=top_configs,
                window_days=args.wf_window_days,
                markets=markets,
            ))
        wf_section = "\n\n--- WALK-FORWARD VALIDATION ---\n" + buf.getvalue()
        body = body + wf_section

    # Add date to subject for daily report clarity
    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"{subject} — {today}"

    _send_email_if_configured(subject, body)


if __name__ == "__main__":
    main()
