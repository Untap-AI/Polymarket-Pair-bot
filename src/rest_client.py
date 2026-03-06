"""CLOB REST API wrapper for fallback and validation.

Provides async wrappers for the Polymarket CLOB REST endpoints.
Prefer WebSocket data; REST is used for health checks and fallback only.
"""

import logging
from typing import Optional

import aiohttp

from .price_utils import price_to_points

logger = logging.getLogger(__name__)

CLOB_BASE_URL = "https://clob.polymarket.com"


class CLOBRestClient:
    """Async wrapper for Polymarket CLOB REST endpoints."""

    def __init__(self, base_url: str = CLOB_BASE_URL):
        self.base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()

    # --- Endpoints ---

    async def get_server_time(self) -> str:
        """GET /time — returns server timestamp string."""
        session = await self._ensure_session()
        async with session.get(f"{self.base_url}/time") as resp:
            resp.raise_for_status()
            return await resp.text()

    async def get_book(self, token_id: str) -> dict:
        """GET /book?token_id={id} — full orderbook for one token.

        NOTE: This endpoint can return stale data on active markets.
        Prefer WebSocket `book` events.
        """
        session = await self._ensure_session()
        async with session.get(
            f"{self.base_url}/book", params={"token_id": token_id}
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_books_batch(self, token_ids: list[str]) -> list[dict]:
        """POST /books — batch orderbook retrieval."""
        session = await self._ensure_session()
        body = [{"token_id": tid} for tid in token_ids]
        async with session.post(f"{self.base_url}/books", json=body) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def get_price(self, token_id: str, side: str = "BUY") -> Optional[int]:
        """GET /price — returns best price in points for the given side."""
        session = await self._ensure_session()
        async with session.get(
            f"{self.base_url}/price",
            params={"token_id": token_id, "side": side},
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            price_str = data.get("price")
            return price_to_points(price_str) if price_str else None

    async def get_midpoint(self, token_id: str) -> Optional[int]:
        """GET /midpoint — returns midpoint price in points."""
        session = await self._ensure_session()
        async with session.get(
            f"{self.base_url}/midpoint", params={"token_id": token_id}
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            mid_str = data.get("mid")
            return price_to_points(mid_str) if mid_str else None

    async def get_tick_size(self, token_id: str) -> Optional[int]:
        """GET /tick-size — returns tick size in points."""
        session = await self._ensure_session()
        async with session.get(
            f"{self.base_url}/tick-size", params={"token_id": token_id}
        ) as resp:
            resp.raise_for_status()
            data = await resp.json()
            ts_str = data.get("minimum_tick_size")
            return price_to_points(str(ts_str)) if ts_str else None

    async def get_orderbook_depths(
        self,
        yes_token_id: str,
        no_token_id: str,
        tick_size_points: int,
        ticks: int = 2,
    ) -> tuple[Optional[float], Optional[float]]:
        """Fetch cumulative ask depth within ``ticks`` ticks of best ask for YES and NO.

        Uses ``POST /books`` (single batched call). Returns ``(yes_depth, no_depth)``.
        Returns ``(None, None)`` on any error.
        """
        try:
            books = await self.get_books_batch([yes_token_id, no_token_id])
        except Exception as exc:
            logger.warning("Orderbook depth fetch failed: %s", exc)
            return None, None

        if not isinstance(books, list) or len(books) < 2:
            return None, None

        tick_price = ticks * tick_size_points / 100.0

        def _cumulative_ask_depth(book: dict) -> Optional[float]:
            asks = book.get("asks", [])
            if not asks:
                return None
            try:
                best_ask = min(float(a["price"]) for a in asks)
                depth = sum(
                    float(a["size"])
                    for a in asks
                    if float(a["price"]) <= best_ask + tick_price
                )
                return depth if depth > 0 else None
            except (KeyError, ValueError, TypeError):
                return None

        # Map by asset_id (fall back to positional order if unavailable)
        book_by_token: dict[str, dict] = {}
        for b in books:
            aid = str(b.get("asset_id") or b.get("market") or "")
            if aid:
                book_by_token[aid] = b

        yes_book = book_by_token.get(yes_token_id) or books[0]
        no_book = book_by_token.get(no_token_id) or books[1]
        return _cumulative_ask_depth(yes_book), _cumulative_ask_depth(no_book)

    # --- Health check ---

    async def check_health(self) -> bool:
        """Return True if the CLOB API is reachable (GET /time succeeds)."""
        try:
            await self.get_server_time()
            return True
        except Exception as e:
            logger.warning("CLOB API health check failed: %s", e)
            return False
