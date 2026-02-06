"""WebSocket client for real-time Polymarket CLOB orderbook data.

Connects to the CLOB WebSocket, subscribes to token orderbooks,
maintains local best-bid/ask state, and handles reconnection with
exponential backoff.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Callable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from .models import TokenOrderbook
from .price_utils import price_to_points

logger = logging.getLogger(__name__)


class WebSocketClient:
    """Manages a WebSocket connection to the Polymarket CLOB for real-time data.

    Maintains a ``TokenOrderbook`` per subscribed asset_id, updated live
    from ``book``, ``price_change``, and ``last_trade_price`` events.
    """

    def __init__(
        self,
        url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        heartbeat_interval: int = 30,
        reconnect_max_delay: int = 60,
    ):
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_max_delay = reconnect_max_delay

        # Orderbook state per asset_id
        self._orderbooks: dict[str, TokenOrderbook] = {}

        # Subscribed asset IDs
        self._subscribed_ids: set[str] = set()

        # Connection state
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._connected = asyncio.Event()
        self._running = False
        self._last_message_time: Optional[float] = None

        # Background tasks
        self._listen_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    @property
    def last_message_time(self) -> Optional[float]:
        """Epoch timestamp of the last received WS message."""
        return self._last_message_time

    def get_orderbook(self, asset_id: str) -> Optional[TokenOrderbook]:
        """Return the current orderbook for a token, or None."""
        return self._orderbooks.get(asset_id)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect_and_subscribe(self, asset_ids: list[str]) -> None:
        """Connect to the WebSocket and subscribe to the given asset IDs.

        Blocks until the first successful connection or raises on timeout.
        """
        self._subscribed_ids = set(asset_ids)
        for aid in asset_ids:
            if aid not in self._orderbooks:
                self._orderbooks[aid] = TokenOrderbook(asset_id=aid)

        self._running = True
        self._listen_task = asyncio.create_task(self._connection_loop())

        # Wait for the first successful connection
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=30)
            logger.info(
                "WebSocket connected and subscribed to %d asset(s)", len(asset_ids)
            )
        except asyncio.TimeoutError:
            logger.error("WebSocket connection timed out after 30 s")
            raise

    async def subscribe(self, asset_ids: list[str]) -> None:
        """Subscribe to additional asset IDs on the existing connection."""
        for aid in asset_ids:
            if aid not in self._orderbooks:
                self._orderbooks[aid] = TokenOrderbook(asset_id=aid)
            self._subscribed_ids.add(aid)

        if self._ws and self._connected.is_set():
            msg = json.dumps({"assets_ids": asset_ids, "operation": "subscribe"})
            await self._ws.send(msg)
            logger.info("Subscribed to %d additional asset(s)", len(asset_ids))

    async def unsubscribe(self, asset_ids: list[str]) -> None:
        """Unsubscribe from asset IDs on the existing connection."""
        for aid in asset_ids:
            self._subscribed_ids.discard(aid)
            self._orderbooks.pop(aid, None)

        if self._ws and self._connected.is_set():
            msg = json.dumps({"assets_ids": asset_ids, "operation": "unsubscribe"})
            await self._ws.send(msg)
            logger.info("Unsubscribed from %d asset(s)", len(asset_ids))

    async def stop(self) -> None:
        """Gracefully stop the WebSocket client."""
        self._running = False

        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        if self._listen_task and not self._listen_task.done():
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass

        self._connected.clear()
        logger.info("WebSocket client stopped")

    # ------------------------------------------------------------------
    # Connection loop (reconnects automatically)
    # ------------------------------------------------------------------

    async def _connection_loop(self) -> None:
        """Main loop: connect → subscribe → listen → reconnect on failure."""
        backoff = 1

        while self._running:
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=self.heartbeat_interval,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    backoff = 1  # reset on success

                    # Send subscription message
                    sub_msg = {
                        "assets_ids": list(self._subscribed_ids),
                        "type": "market",
                    }
                    await ws.send(json.dumps(sub_msg))
                    logger.info(
                        "Sent subscription for %d asset(s)", len(self._subscribed_ids)
                    )

                    self._connected.set()

                    # Listen for messages until disconnect
                    async for raw_msg in ws:
                        if not self._running:
                            break
                        self._last_message_time = time.time()
                        self._handle_raw_message(raw_msg)

            except ConnectionClosed as e:
                logger.warning(
                    "WebSocket closed: code=%s reason=%s", e.code, e.reason
                )
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("WebSocket error: %s", e)
            finally:
                self._connected.clear()
                self._ws = None

            if self._running:
                logger.info("Reconnecting in %d s …", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.reconnect_max_delay)

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _handle_raw_message(self, raw_msg: str) -> None:
        """Parse raw WS text and dispatch events."""
        try:
            data = json.loads(raw_msg)
        except json.JSONDecodeError as e:
            logger.warning("Failed to parse WS message: %s", e)
            return

        # Polymarket may send a single event dict or a list of events
        if isinstance(data, list):
            for event in data:
                if isinstance(event, dict):
                    self._process_event(event)
        elif isinstance(data, dict):
            self._process_event(data)

    def _process_event(self, event: dict) -> None:
        """Route one parsed event to the appropriate handler."""
        event_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if not asset_id or asset_id not in self._orderbooks:
            return

        ob = self._orderbooks[asset_id]

        if event_type == "book":
            self._handle_book_event(ob, event)
        elif event_type == "price_change":
            self._handle_price_change(ob, event)
        elif event_type == "last_trade_price":
            self._handle_last_trade(ob, event)
        elif event_type == "tick_size_change":
            logger.info("Tick size change for %s…: %s", asset_id[:16], event)
        else:
            logger.debug("Unknown WS event type: %s", event_type)

    # --- Event handlers ---

    def _handle_book_event(self, ob: TokenOrderbook, event: dict) -> None:
        """Full orderbook snapshot — find best bid (highest) and best ask (lowest)."""
        bids = event.get("bids", [])
        asks = event.get("asks", [])

        if bids:
            # Best bid = highest price among all bids
            best = max(bids, key=lambda b: float(b["price"]))
            ob.best_bid = price_to_points(best["price"])
            ob.best_bid_size = best.get("size")
        else:
            ob.best_bid = None
            ob.best_bid_size = None

        if asks:
            # Best ask = lowest price among all asks
            best = min(asks, key=lambda a: float(a["price"]))
            ob.best_ask = price_to_points(best["price"])
            ob.best_ask_size = best.get("size")
        else:
            ob.best_ask = None
            ob.best_ask_size = None

        ob.last_update = datetime.now(timezone.utc)

        logger.debug(
            "Book snapshot %s...: bid=%s ask=%s (from %d bids, %d asks)",
            ob.asset_id[:16],
            ob.best_bid,
            ob.best_ask,
            len(bids),
            len(asks),
        )

    def _handle_price_change(self, ob: TokenOrderbook, event: dict) -> None:
        """Lightweight best-bid/ask update."""
        changes = event.get("price_changes", [])
        if not changes:
            return

        change = changes[0]
        bid_str = change.get("best_bid")
        ask_str = change.get("best_ask")

        if bid_str:
            ob.best_bid = price_to_points(bid_str)
        if ask_str:
            ob.best_ask = price_to_points(ask_str)

        ob.last_update = datetime.now(timezone.utc)

    def _handle_last_trade(self, ob: TokenOrderbook, event: dict) -> None:
        """Last trade price event."""
        price_str = event.get("price")
        if price_str:
            ob.last_trade_price = price_to_points(price_str)
            ob.last_update = datetime.now(timezone.utc)
