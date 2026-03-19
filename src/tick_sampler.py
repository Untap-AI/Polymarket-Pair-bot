"""Async sampling loop that captures orderbook ticks at a fixed interval.

Reads current WebSocket state every N seconds and records an OrderbookTick
into the TickStore. Runs independently of the measurement cycle loop.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .models import MarketInfo, OrderbookTick
from .tick_store import TickStore
from .websocket_client import WebSocketClient

logger = logging.getLogger(__name__)


class TickSampler:
    """Lightweight sampler that captures orderbook state at a fixed interval."""

    def __init__(
        self,
        market_info: MarketInfo,
        ws_client: WebSocketClient,
        tick_store: TickStore,
        interval: float = 2.0,
    ):
        self._market_info = market_info
        self._ws = ws_client
        self._tick_store = tick_store
        self._interval = interval
        self._running = False
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        """Start the sampling loop as a background asyncio task."""
        if self._task is not None:
            return
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(
            "Tick sampler started for %s (interval=%.1fs)",
            self._market_info.market_slug,
            self._interval,
        )

    def stop(self) -> None:
        """Signal the sampling loop to exit."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _run(self) -> None:
        """Sample loop: capture tick → record → sleep."""
        try:
            while self._running:
                tick = self._capture_tick()
                if tick is not None:
                    self._tick_store.record_tick(tick)
                await asyncio.sleep(self._interval)
        except asyncio.CancelledError:
            pass

    def _capture_tick(self) -> OrderbookTick | None:
        """Read current WS orderbook state into an OrderbookTick."""
        now = datetime.now(timezone.utc)
        mi = self._market_info
        time_remaining = (mi.settlement_time - now).total_seconds()

        yes_ob = self._ws.get_orderbook(mi.yes_token_id)
        no_ob = self._ws.get_orderbook(mi.no_token_id)

        if yes_ob is None or no_ob is None:
            return None

        def _size(s: str | None) -> float | None:
            try:
                return float(s) if s is not None else None
            except (ValueError, TypeError):
                return None

        return OrderbookTick(
            timestamp=now,
            market_id=mi.market_slug,
            crypto_asset=mi.crypto_asset,
            time_remaining=time_remaining,
            yes_best_bid=yes_ob.best_bid,
            yes_best_ask=yes_ob.best_ask,
            no_best_bid=no_ob.best_bid,
            no_best_ask=no_ob.best_ask,
            yes_bid_size=_size(yes_ob.best_bid_size),
            yes_ask_size=_size(yes_ob.best_ask_size),
            no_bid_size=_size(no_ob.best_bid_size),
            no_ask_size=_size(no_ob.best_ask_size),
        )
