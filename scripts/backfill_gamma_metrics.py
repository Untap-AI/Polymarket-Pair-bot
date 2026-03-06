#!/usr/bin/env python3
"""Backfill volume24hr, liquidity, and open_interest for existing Markets rows.

Queries the Polymarket Gamma API for each market slug and records the three
Gamma metrics into the Markets table.

CAVEAT: The Gamma API reflects the *current* (settled) state of the market,
not discovery-time values. Volume and liquidity for a 15-min market will be
frozen at settlement, so these values are a reasonable proxy but may differ
slightly from what was live when the market was first discovered.

Usage:
    python scripts/backfill_gamma_metrics.py              # dry-run (default)
    python scripts/backfill_gamma_metrics.py --execute    # apply updates
    python scripts/backfill_gamma_metrics.py --db-url <url>
    python scripts/backfill_gamma_metrics.py --limit 50   # process at most N markets
    python scripts/backfill_gamma_metrics.py --concurrency 5
    python scripts/backfill_gamma_metrics.py --batch-delay 2.0
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
DEFAULT_CONCURRENCY = 5
DEFAULT_BATCH_DELAY = 2.0


def _resolve_db_url(args) -> str:
    url = (
        getattr(args, "db_url", None)
        or os.environ.get("DATABASE_URL_SESSION")
        or os.environ.get("DATABASE_URL")
    )
    if not url:
        print("Error: No database URL provided.")
        print("  Use --db-url or set DATABASE_URL_SESSION or DATABASE_URL.")
        sys.exit(1)
    if "postgres" not in url.lower():
        print("Error: This script only supports PostgreSQL. Got:", url[:30])
        sys.exit(1)
    return url


def _safe_float(val) -> "float | None":
    try:
        return float(val) if val is not None else None
    except (ValueError, TypeError):
        return None


def _parse_gamma_metrics(event: dict) -> "tuple[float | None, float | None, float | None]":
    """Extract (volume24hr, liquidity, open_interest) from a Gamma API event dict."""
    markets = event.get("markets", [])
    market = markets[0] if markets else {}

    volume24hr = _safe_float(event.get("volume24hr") or market.get("volume24hr"))
    liquidity = _safe_float(event.get("liquidity") or market.get("liquidity"))
    open_interest = _safe_float(event.get("openInterest") or market.get("openInterest"))
    return volume24hr, liquidity, open_interest


def _parse_market_metrics(market: dict) -> "tuple[float | None, float | None, float | None]":
    """Extract (volume24hr, liquidity, open_interest) from a Gamma API market dict."""
    volume24hr = _safe_float(market.get("volume24hr"))
    liquidity = _safe_float(market.get("liquidity"))
    open_interest = _safe_float(market.get("openInterest"))
    return volume24hr, liquidity, open_interest


async def fetch_metrics_for_slug(
    session, slug: str, condition_id: str, semaphore: asyncio.Semaphore
) -> "tuple[str, float | None, float | None, float | None, str | None]":
    """Query Gamma API and return (slug, volume24hr, liquidity, open_interest, skip_reason).

    Tries /events?slug first, then falls back to /markets?condition_id.
    """
    import aiohttp

    async with semaphore:
        # --- Attempt 1: event slug ---
        try:
            async with session.get(
                f"{GAMMA_API_BASE}/events",
                params={"slug": slug},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                events = await resp.json()

            if events and isinstance(events, list):
                volume24hr, liquidity, open_interest = _parse_gamma_metrics(events[0])
                if not (volume24hr is None and liquidity is None and open_interest is None):
                    return (slug, volume24hr, liquidity, open_interest, None)
                slug_fail = "all_null"
            else:
                slug_fail = "not_found"
        except Exception as exc:
            slug_fail = f"error:{type(exc).__name__}"

        # --- Attempt 2: condition_id fallback ---
        if not condition_id:
            return (slug, None, None, None, f"slug:{slug_fail},no_condition_id")

        try:
            async with session.get(
                f"{GAMMA_API_BASE}/markets",
                params={"condition_id": condition_id},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                markets = await resp.json()

            if markets and isinstance(markets, list):
                volume24hr, liquidity, open_interest = _parse_market_metrics(markets[0])
                if not (volume24hr is None and liquidity is None and open_interest is None):
                    return (slug, volume24hr, liquidity, open_interest, None)
                return (slug, None, None, None, f"slug:{slug_fail},cid:all_null")
            else:
                return (slug, None, None, None, f"slug:{slug_fail},cid:not_found")
        except Exception as exc:
            return (slug, None, None, None, f"slug:{slug_fail},cid:error:{type(exc).__name__}")


async def run(args) -> None:
    import aiohttp
    import asyncpg

    url = _resolve_db_url(args)
    dry_run = not args.execute
    limit = getattr(args, "limit", None)
    concurrency = getattr(args, "concurrency", DEFAULT_CONCURRENCY)
    batch_delay = getattr(args, "batch_delay", DEFAULT_BATCH_DELAY)

    print(f"Mode: {'DRY RUN' if dry_run else 'EXECUTE'}")
    print(f"Concurrency: {concurrency} | Batch delay: {batch_delay}s")

    pool = await asyncpg.create_pool(url, min_size=1, max_size=5, statement_cache_size=0)
    try:
        query = (
            "SELECT market_id, condition_id FROM Markets "
            "WHERE volume24hr IS NULL AND liquidity IS NULL "
            "ORDER BY settlement_time"
        )
        if limit:
            query += f" LIMIT {int(limit)}"

        rows = await pool.fetch(query)
        market_rows = [(r["market_id"], r["condition_id"] or "") for r in rows]

        print(f"Markets missing Gamma metrics: {len(market_rows)}")
        if not market_rows:
            print("Nothing to backfill.")
            return

        semaphore = asyncio.Semaphore(concurrency)
        updated = 0
        skipped = 0

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(market_rows), concurrency):
                batch = market_rows[batch_start : batch_start + concurrency]
                batch_num = (batch_start // concurrency) + 1
                total_batches = (len(market_rows) + concurrency - 1) // concurrency

                tasks = [
                    fetch_metrics_for_slug(session, slug, cid, semaphore)
                    for slug, cid in batch
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                to_update: list[tuple] = []
                for i, r in enumerate(results):
                    slug = batch[i][0]
                    if isinstance(r, Exception):
                        skipped += 1
                        print(f"    SKIP {slug}: exception:{r}")
                        continue
                    _, volume24hr, liquidity, open_interest, skip_reason = r
                    if skip_reason is not None:
                        skipped += 1
                        print(f"    SKIP {slug}: {skip_reason}")
                    else:
                        to_update.append((volume24hr, liquidity, open_interest, slug))

                if to_update and not dry_run:
                    async with pool.acquire() as conn:
                        await conn.executemany(
                            "UPDATE Markets SET volume24hr=$1, liquidity=$2, open_interest=$3 WHERE market_id=$4",
                            to_update,
                        )
                updated += len(to_update)

                done = min(batch_start + concurrency, len(market_rows))
                print(
                    f"  Batch {batch_num}/{total_batches}: "
                    f"{len(to_update)} updated, {len(batch) - len(to_update)} skipped "
                    f"({done}/{len(market_rows)} total)"
                )

                if batch_start + concurrency < len(market_rows):
                    await asyncio.sleep(batch_delay)

        print()
        print("Results:")
        print(f"  Would update / updated : {updated}")
        print(f"  Skipped (no data found): {skipped}")
        if dry_run:
            print("\nDry-run — nothing written. Pass --execute to apply.")
        else:
            print(f"\nDone. {updated} market(s) updated.")
    finally:
        await pool.close()


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(
        description="Backfill Gamma API metrics (volume24hr, liquidity, open_interest) for Markets"
    )
    parser.add_argument("--db-url", default=None, help="PostgreSQL connection URL")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Write updates to the database (default is dry-run)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process at most N markets",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        metavar="N",
        help=f"Max parallel API requests per batch (default: {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--batch-delay",
        type=float,
        default=DEFAULT_BATCH_DELAY,
        metavar="SECS",
        help=f"Seconds to wait between batches (default: {DEFAULT_BATCH_DELAY})",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
