"""Per-asset manager: continuous discover → monitor → settle → rotate loop.

Each crypto asset (BTC, ETH, SOL, XRP) gets one ``AssetManager`` coroutine
that runs for the entire lifetime of the bot.  It discovers the current
15-minute market, monitors it via a ``MarketMonitor``, and when that market
settles it immediately discovers and transitions to the next window.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import Optional

from .config import AppConfig
from .database import Database
from .market_discovery import MarketDiscovery, WINDOW_SECONDS
from .market_monitor import MarketMonitor, MarketSummary
from .models import MarketInfo, ParameterSet
from .rest_client import CLOBRestClient
from .websocket_client import WebSocketClient

logger = logging.getLogger(__name__)

# Discovery retry settings
MAX_DISCOVERY_RETRIES = 40       # 40 × ~5s = ~200s max wait
DISCOVERY_RETRY_BASE_DELAY = 2   # seconds


class AssetManager:
    """Manages the continuous lifecycle of one crypto asset's 15-min markets.

    Public properties (``markets_monitored``, ``total_attempts``, etc.)
    are read by the dashboard and status reporter in ``main.py``.
    """

    def __init__(
        self,
        crypto_asset: str,
        params_list: list[ParameterSet],
        config: AppConfig,
        database: Database,
        rest_client: CLOBRestClient,
        shutdown_event: asyncio.Event,
        event_log: Optional[deque] = None,
    ):
        self.crypto_asset = crypto_asset
        self.params_list = params_list
        self.config = config
        self.db = database
        self.rest = rest_client
        self._shutdown = shutdown_event
        self._event_log = event_log

        self._discovery = MarketDiscovery()

        # Runtime state
        self._current_monitor: Optional[MarketMonitor] = None
        self._current_market: Optional[MarketInfo] = None
        self._summaries: list[MarketSummary] = []
        self._status: str = "starting"
        self._last_slug_ts: Optional[int] = None

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _push_event(self, msg: str) -> None:
        if self._event_log is not None:
            self._event_log.append((
                datetime.now(timezone.utc),
                self.crypto_asset.upper(),
                msg,
            ))

    # ------------------------------------------------------------------
    # Aggregate properties (for status / session summary)
    # ------------------------------------------------------------------

    @property
    def markets_monitored(self) -> int:
        return len(self._summaries)

    @property
    def total_attempts(self) -> int:
        return sum(s.total_attempts for s in self._summaries)

    @property
    def total_pairs(self) -> int:
        return sum(s.total_pairs for s in self._summaries)

    @property
    def total_failed(self) -> int:
        return sum(s.total_failed for s in self._summaries)

    @property
    def status_line(self) -> str:
        """One-line status string for console output (Phase 2 fallback)."""
        tag = self.crypto_asset.upper()

        if self._status == "monitoring" and self._current_monitor:
            m = self._current_monitor
            ev = m.evaluator
            remaining = (
                m.market_info.settlement_time - datetime.now(timezone.utc)
            ).total_seconds()
            remaining = max(0, remaining)
            mins, secs = divmod(int(remaining), 60)
            total_att = ev.total_attempts
            total_pair = ev.total_pairs
            pct = (total_pair / max(1, total_att)) * 100
            return (
                f"{tag}: {m.market_info.market_slug} | "
                f"{mins}m {secs:02d}s left | "
                f"cycle {m.cycles_run}/{m.total_planned_cycles} | "
                f"attempts: {total_att} | pairs: {total_pair} ({pct:.0f}%)"
            )

        if self._status == "discovering":
            return f"{tag}: discovering next market..."

        return f"{tag}: {self._status}"

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run continuously until shutdown is requested."""
        logger.info("Asset manager started for %s", self.crypto_asset.upper())

        try:
            while not self._shutdown.is_set():
                # --- Discover ---
                self._status = "discovering"
                market_info = await self._discover_with_retry()
                if market_info is None:
                    break

                # --- Monitor ---
                self._status = "monitoring"
                self._current_market = market_info
                self._last_slug_ts = self._extract_slug_ts(market_info.market_slug)

                ws_client = WebSocketClient(
                    url=self.config.websocket.url,
                    heartbeat_interval=self.config.websocket.heartbeat_interval_seconds,
                    reconnect_max_delay=self.config.websocket.reconnect_max_delay_seconds,
                )
                monitor = MarketMonitor(
                    market_info=market_info,
                    params_list=self.params_list,
                    config=self.config,
                    database=self.db,
                    ws_client=ws_client,
                    rest_client=self.rest,
                    shutdown_event=self._shutdown,
                    event_log=self._event_log,
                )
                self._current_monitor = monitor

                summary = await monitor.run()

                self._current_monitor = None
                self._current_market = None
                self._summaries.append(summary)

                self._log_market_complete(summary)

                # Brief pause before discovering next
                if not self._shutdown.is_set():
                    self._push_event(
                        f"Market {summary.market_id} settled -> discovering next..."
                    )
                    await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("Asset manager cancelled for %s", self.crypto_asset.upper())
        finally:
            await self._discovery.close()
            self._status = "stopped"
            logger.info(
                "Asset manager stopped for %s — %d markets, %d attempts, %d pairs",
                self.crypto_asset.upper(),
                self.markets_monitored,
                self.total_attempts,
                self.total_pairs,
            )

    # ------------------------------------------------------------------
    # Discovery with retry
    # ------------------------------------------------------------------

    async def _discover_with_retry(self) -> Optional[MarketInfo]:
        """Try to find the current/next market, retrying with backoff."""
        for attempt in range(MAX_DISCOVERY_RETRIES):
            if self._shutdown.is_set():
                return None

            market = await self._discover_next_market()
            if market is not None:
                self._push_event(f"Discovered {market.market_slug}")
                return market

            delay = min(5, DISCOVERY_RETRY_BASE_DELAY + attempt)
            logger.info(
                "%s: no market found (attempt %d/%d), retrying in %ds",
                self.crypto_asset.upper(),
                attempt + 1,
                MAX_DISCOVERY_RETRIES,
                delay,
            )

            try:
                await asyncio.wait_for(
                    asyncio.shield(self._shutdown.wait()), timeout=delay
                )
                return None
            except asyncio.TimeoutError:
                pass

        logger.warning(
            "%s: failed to find market after %d retries",
            self.crypto_asset.upper(),
            MAX_DISCOVERY_RETRIES,
        )
        return None

    async def _discover_next_market(self) -> Optional[MarketInfo]:
        """Try targeted slug lookup first, then fall back to general discovery."""
        market_type = self.config.markets.market_type

        if self._last_slug_ts is not None:
            next_ts = self._last_slug_ts + WINDOW_SECONDS
            next_slug = f"{self.crypto_asset}-updown-{market_type}-{next_ts}"
            market = await self._discovery.find_market_by_slug(
                next_slug, self.crypto_asset
            )
            if market is not None:
                return market

        return await self._discovery.find_active_market(
            self.crypto_asset, market_type
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_slug_ts(slug: str) -> Optional[int]:
        """Extract the Unix timestamp from a slug like ``btc-updown-15m-1770356700``."""
        try:
            return int(slug.rsplit("-", 1)[1])
        except (ValueError, IndexError):
            return None

    def _log_market_complete(self, summary: MarketSummary) -> None:
        tag = self.crypto_asset.upper()
        logger.info(
            "[%s] Market %s complete — %d cycles, %d attempts, %d pairs (%.0f%%)",
            tag,
            summary.market_id,
            summary.total_cycles,
            summary.total_attempts,
            summary.total_pairs,
            summary.pair_rate * 100,
        )
