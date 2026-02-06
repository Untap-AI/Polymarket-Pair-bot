"""Gamma API: discover active 15-minute crypto prediction markets.

These markets are EVENTS in Polymarket (not standalone markets). Each event
slug follows the pattern ``{crypto}-updown-15m-{unix_timestamp}`` where the
timestamp is the **window start time**. Settlement = start + 900 seconds.

Discovery approach:
  1. Compute the expected slug for the current 15-min window
  2. Query ``GET /events?slug={slug}`` on the Gamma events endpoint
  3. Parse the nested market (clobTokenIds, outcomes, endDate, tick size)
  4. Fall back to broader search if the direct slug query misses
"""

import json
import logging
import time as _time
from datetime import datetime, timezone
from typing import Optional

import aiohttp

from .models import MarketInfo
from .price_utils import price_to_points

logger = logging.getLogger(__name__)

GAMMA_API_BASE = "https://gamma-api.polymarket.com"

# 15-minute windows are on 900-second boundaries
WINDOW_SECONDS = 900


class MarketDiscovery:
    """Discovers active Polymarket 15-minute crypto markets via the Gamma API."""

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Main discovery entry point
    # ------------------------------------------------------------------

    async def find_active_market(
        self, crypto_asset: str, market_type: str = "15m"
    ) -> Optional[MarketInfo]:
        """Find the currently active 15-min market for a crypto asset.

        Strategy:
          1. Compute the expected event slug for the current window
          2. Query the Gamma ``/events`` endpoint by exact slug
          3. If not found, try adjacent windows (next, previous)
          4. If still not found, fall back to a broader search
        """
        now_ts = int(_time.time())
        window_start = now_ts - (now_ts % WINDOW_SECONDS)

        # Try current window, then next, then previous
        candidates = [window_start, window_start + WINDOW_SECONDS, window_start - WINDOW_SECONDS]

        for ts in candidates:
            slug = f"{crypto_asset}-updown-{market_type}-{ts}"
            result = await self._query_event_by_slug(slug, crypto_asset)
            if result is not None:
                return result

        # Fallback: broader search
        logger.info(
            "Direct slug lookup failed for %s — trying broader search",
            crypto_asset.upper(),
        )
        return await self._search_events_broadly(crypto_asset, market_type)

    async def find_market_by_slug(
        self, slug: str, crypto_asset: str
    ) -> Optional[MarketInfo]:
        """Look up a specific market by its exact event slug.

        Useful for targeted next-market discovery after a settlement,
        where the expected slug timestamp is known.
        """
        return await self._query_event_by_slug(slug, crypto_asset)

    # ------------------------------------------------------------------
    # Strategy 1: Direct slug lookup (fast path)
    # ------------------------------------------------------------------

    async def _query_event_by_slug(
        self, slug: str, crypto_asset: str
    ) -> Optional[MarketInfo]:
        """Query ``GET /events?slug={slug}`` and parse the result."""
        session = await self._ensure_session()

        try:
            async with session.get(
                f"{GAMMA_API_BASE}/events", params={"slug": slug}
            ) as resp:
                resp.raise_for_status()
                events = await resp.json()

            if not events or not isinstance(events, list):
                return None

            event = events[0]

            # Skip closed/resolved events
            if event.get("closed", False):
                logger.debug("Event %s is closed, skipping", slug)
                return None

            return self._parse_event(event, crypto_asset)

        except aiohttp.ClientError as e:
            logger.warning("Gamma API request failed for slug %s: %s", slug, e)
            return None
        except Exception as e:
            logger.error("Unexpected error querying slug %s: %s", slug, e)
            return None

    # ------------------------------------------------------------------
    # Strategy 2: Broader search (fallback)
    # ------------------------------------------------------------------

    async def _search_events_broadly(
        self, crypto_asset: str, market_type: str
    ) -> Optional[MarketInfo]:
        """Search for open events matching the slug pattern."""
        session = await self._ensure_session()
        slug_pattern = f"{crypto_asset}-updown-{market_type}"

        try:
            params = {
                "closed": "false",
                "limit": "100",
                "order": "startDate",
                "ascending": "true",
            }
            async with session.get(
                f"{GAMMA_API_BASE}/events", params=params
            ) as resp:
                resp.raise_for_status()
                events = await resp.json()

            if not isinstance(events, list):
                return None

            now = datetime.now(timezone.utc)
            best: Optional[MarketInfo] = None

            for event in events:
                event_slug = (event.get("slug", "") or "").lower()
                if slug_pattern not in event_slug:
                    continue
                if event.get("closed", False):
                    continue

                result = self._parse_event(event, crypto_asset)
                if result is None:
                    continue

                # Prefer the market whose window contains "now"
                start_str = event.get("startTime", "")
                if start_str:
                    start_dt = datetime.fromisoformat(
                        start_str.replace("Z", "+00:00")
                    )
                    if start_dt <= now < result.settlement_time:
                        return result  # Currently live — use immediately

                # Otherwise keep the soonest upcoming
                if best is None or result.settlement_time < best.settlement_time:
                    best = result

            if best:
                logger.info("Broad search found upcoming market: %s", best.market_slug)
            else:
                logger.info(
                    "No active %s market found in broad search",
                    crypto_asset.upper(),
                )
            return best

        except aiohttp.ClientError as e:
            logger.error("Gamma API broad search failed: %s", e)
            return None
        except Exception as e:
            logger.error("Unexpected error in broad search: %s", e)
            return None

    # ------------------------------------------------------------------
    # Event parsing
    # ------------------------------------------------------------------

    def _parse_event(
        self, event: dict, crypto_asset: str
    ) -> Optional[MarketInfo]:
        """Parse a Gamma API event (with nested market) into MarketInfo.

        The event contains:
          - slug: ``btc-updown-15m-{timestamp}``
          - startTime / endDate: ISO timestamps for the window
          - markets[0]: the single binary market with clobTokenIds & outcomes
        """
        try:
            event_slug = event.get("slug", "")

            # --- Nested market ---
            markets = event.get("markets", [])
            if not markets:
                logger.warning("Event %s has no nested markets", event_slug)
                return None
            market = markets[0]

            condition_id = str(market.get("conditionId", "") or "")

            # --- Token IDs ---
            yes_token_id, no_token_id = self._extract_token_ids(market, event_slug)
            if not yes_token_id or not no_token_id:
                return None

            # --- Settlement time ---
            # Prefer the event-level endDate (has full ISO timestamp)
            settlement_time = self._parse_settlement_time(event, market, event_slug)
            if settlement_time is None:
                return None

            # Skip already-settled
            if settlement_time <= datetime.now(timezone.utc):
                logger.debug("Event %s already settled", event_slug)
                return None

            # --- Tick size ---
            tick_str = str(market.get("orderPriceMinTickSize", "0.01") or "0.01")
            tick_size_points = price_to_points(tick_str)
            # Resolved markets sometimes report 0.001 tick; live ones use 0.01
            if tick_size_points < 1:
                tick_size_points = 1  # Floor to 1 point ($0.01)

            # --- Check liveness ---
            accepting = market.get("acceptingOrders", False)
            closed = market.get("closed", False)

            market_info = MarketInfo(
                market_slug=event_slug,
                condition_id=condition_id,
                crypto_asset=crypto_asset,
                yes_token_id=yes_token_id,
                no_token_id=no_token_id,
                settlement_time=settlement_time,
                tick_size_points=tick_size_points,
                active=not closed,
                accepting_orders=bool(accepting),
            )

            logger.info(
                "Discovered market: %s | settlement=%s | tick=%dpt | accepting=%s "
                "| YES=%s… | NO=%s…",
                event_slug,
                settlement_time.strftime("%H:%M:%S UTC"),
                tick_size_points,
                accepting,
                yes_token_id[:16],
                no_token_id[:16],
            )
            return market_info

        except (KeyError, ValueError, TypeError) as e:
            logger.error("Failed to parse event %s: %s", event.get("slug", "?"), e)
            return None

    # ------------------------------------------------------------------

    def _parse_settlement_time(
        self, event: dict, market: dict, slug: str
    ) -> Optional[datetime]:
        """Extract the settlement (end) time from event or market fields.

        Priority:
          1. Event ``endDate`` (full ISO with time)
          2. Market ``endDateIso`` (sometimes date-only)
          3. Derive from slug timestamp + 900s
        """
        # Try event endDate (usually has full datetime)
        end_str = event.get("endDate", "")
        if end_str and "T" in str(end_str):
            return datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))

        # Try market endDateIso
        end_iso = market.get("endDateIso", "")
        if end_iso and "T" in str(end_iso):
            return datetime.fromisoformat(str(end_iso).replace("Z", "+00:00"))

        # Derive from slug timestamp (slug ts = window start, settlement = start + 900)
        try:
            parts = slug.rsplit("-", 1)
            if len(parts) == 2:
                slug_ts = int(parts[1])
                settlement_ts = slug_ts + WINDOW_SECONDS
                return datetime.fromtimestamp(settlement_ts, tz=timezone.utc)
        except (ValueError, IndexError):
            pass

        logger.warning("Could not determine settlement time for %s", slug)
        return None

    # ------------------------------------------------------------------

    def _extract_token_ids(
        self, market: dict, slug: str
    ) -> tuple[Optional[str], Optional[str]]:
        """Extract YES (Up) and NO (Down) token IDs from the market dict.

        Uses ``clobTokenIds`` (JSON string or list) paired with ``outcomes``.
        """
        yes_token_id: Optional[str] = None
        no_token_id: Optional[str] = None

        raw_ids = market.get("clobTokenIds", "")
        raw_outcomes = market.get("outcomes", "")

        # Parse JSON strings if needed
        if isinstance(raw_ids, str):
            try:
                raw_ids = json.loads(raw_ids)
            except (json.JSONDecodeError, TypeError):
                raw_ids = []
        if isinstance(raw_outcomes, str):
            try:
                raw_outcomes = json.loads(raw_outcomes)
            except (json.JSONDecodeError, TypeError):
                raw_outcomes = []

        if (
            isinstance(raw_ids, list)
            and isinstance(raw_outcomes, list)
            and len(raw_ids) >= 2
            and len(raw_outcomes) >= 2
        ):
            for token_id, outcome in zip(raw_ids, raw_outcomes):
                outcome_lower = str(outcome).lower()
                if outcome_lower in ("up", "yes"):
                    yes_token_id = str(token_id)
                elif outcome_lower in ("down", "no"):
                    no_token_id = str(token_id)

        if not yes_token_id or not no_token_id:
            logger.warning(
                "Could not extract Up/Down token IDs for %s "
                "(ids=%s, outcomes=%s)",
                slug,
                raw_ids,
                raw_outcomes,
            )

        return yes_token_id, no_token_id
