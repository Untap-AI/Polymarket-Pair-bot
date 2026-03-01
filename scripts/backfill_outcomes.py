#!/usr/bin/env python3
"""Backfill winning_outcome for existing Markets rows using the Polymarket Gamma API.

Each market_id is an event slug (e.g. 'btc-updown-15m-1768502700'). For closed
events the Gamma API returns outcomePrices like ["1", "0"] or ["0", "1"], paired
with outcomes like ["Up", "Down"]. The index with price "1" is the winner.

Uses parallel API fetches with batching and batch DB updates for efficiency.

Usage:
    python scripts/backfill_outcomes.py              # dry-run (default)
    python scripts/backfill_outcomes.py --execute    # apply updates
    python scripts/backfill_outcomes.py --db-url <url>
    python scripts/backfill_outcomes.py --limit 50   # process at most N markets
    python scripts/backfill_outcomes.py --concurrency 8   # parallel API requests
    python scripts/backfill_outcomes.py --batch-delay 2.0 # seconds between batches
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import load_env_file  # noqa: E402

GAMMA_API_BASE = "https://gamma-api.polymarket.com"
# Default concurrency: ~100 req/min API limit → ~5 concurrent with 2s batch delay ≈ 150/min
# Use --concurrency and --batch-delay to tune if you hit rate limits
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


def _parse_outcome(market: dict) -> str | None:
    """Return 'yes', 'no', or None from a Gamma API market object.

    outcomePrices and outcomes are JSON strings that must be parsed.
    The winning side has price "1" (or "1.0").
    """
    raw_prices = market.get("outcomePrices", "")
    raw_outcomes = market.get("outcomes", "")

    if isinstance(raw_prices, str):
        try:
            raw_prices = json.loads(raw_prices)
        except (json.JSONDecodeError, TypeError):
            raw_prices = []
    if isinstance(raw_outcomes, str):
        try:
            raw_outcomes = json.loads(raw_outcomes)
        except (json.JSONDecodeError, TypeError):
            raw_outcomes = []

    if not isinstance(raw_prices, list) or not isinstance(raw_outcomes, list):
        return None
    if len(raw_prices) != len(raw_outcomes):
        return None

    for price_str, outcome_label in zip(raw_prices, raw_outcomes):
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            continue
        if price >= 0.99:  # treat 1 or 1.0 as winner
            label_lower = str(outcome_label).lower()
            if label_lower in ("up", "yes"):
                return "yes"
            if label_lower in ("down", "no"):
                return "no"

    return None


async def fetch_outcome_for_slug(
    session, slug: str, semaphore: asyncio.Semaphore
) -> tuple[str, str | None]:
    """Query Gamma API for the event slug and return (slug, outcome)."""
    import aiohttp

    async with semaphore:
        try:
            async with session.get(
                f"{GAMMA_API_BASE}/events",
                params={"slug": slug},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                resp.raise_for_status()
                events = await resp.json()
        except Exception as exc:
            return (slug, None)

    if not events or not isinstance(events, list):
        return (slug, None)

    event = events[0]
    if not event.get("closed", False):
        return (slug, None)

    markets = event.get("markets", [])
    if not markets:
        return (slug, None)

    return (slug, _parse_outcome(markets[0]))


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
        query = "SELECT market_id FROM Markets WHERE winning_outcome IS NULL ORDER BY settlement_time"
        if limit:
            query += f" LIMIT {int(limit)}"

        rows = await pool.fetch(query)
        market_ids = [r["market_id"] for r in rows]

        print(f"Markets with no outcome recorded: {len(market_ids)}")
        if not market_ids:
            print("Nothing to backfill.")
            return

        semaphore = asyncio.Semaphore(concurrency)
        updated = 0
        skipped = 0

        async with aiohttp.ClientSession() as session:
            for batch_start in range(0, len(market_ids), concurrency):
                batch_slugs = market_ids[batch_start : batch_start + concurrency]
                batch_num = (batch_start // concurrency) + 1
                total_batches = (len(market_ids) + concurrency - 1) // concurrency

                # Fetch this batch in parallel
                tasks = [
                    fetch_outcome_for_slug(session, slug, semaphore)
                    for slug in batch_slugs
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Collect (slug, outcome) for successful fetches; handle exceptions
                to_update: list[tuple[str, str]] = []
                for i, r in enumerate(results):
                    slug = batch_slugs[i]
                    if isinstance(r, Exception):
                        skipped += 1
                        continue
                    _, outcome = r
                    if outcome is None:
                        skipped += 1
                    else:
                        to_update.append((slug, outcome))

                # Batch update DB
                if to_update and not dry_run:
                    async with pool.acquire() as conn:
                        await conn.executemany(
                            "UPDATE Markets SET winning_outcome = $1 WHERE market_id = $2",
                            [(outcome, slug) for slug, outcome in to_update],
                        )
                updated += len(to_update)

                # Progress
                done = min(batch_start + concurrency, len(market_ids))
                print(
                    f"  Batch {batch_num}/{total_batches}: "
                    f"{len(to_update)} resolved, {len(batch_slugs) - len(to_update)} skipped "
                    f"({done}/{len(market_ids)} total)"
                )

                # Rate-limit: delay between batches
                if batch_start + concurrency < len(market_ids):
                    await asyncio.sleep(batch_delay)

        print()
        print(f"Results:")
        print(f"  Would update / updated : {updated}")
        print(f"  Skipped (open/unknown) : {skipped}")
        if dry_run:
            print("\nDry-run — nothing written. Pass --execute to apply.")
        else:
            print(f"\nDone. {updated} market(s) updated.")
    finally:
        await pool.close()


def main() -> None:
    load_env_file()

    parser = argparse.ArgumentParser(
        description="Backfill winning_outcome for Markets rows using the Polymarket Gamma API"
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
        help="Process at most N markets (useful for testing)",
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
        help=f"Seconds to wait between batches for rate limiting (default: {DEFAULT_BATCH_DELAY})",
    )
    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
