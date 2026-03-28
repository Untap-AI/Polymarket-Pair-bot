"""Tick buffer management, Parquet flush, and S3 upload for orderbook tick data.

Buffers OrderbookTick samples in memory per market, periodically flushes them
to Parquet files and uploads to S3 for cheap, queryable time-series storage.
"""

from __future__ import annotations

import asyncio
import logging
import tempfile
from collections import defaultdict, deque
from datetime import date, datetime, timezone, timedelta
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .config import DataConfig
from .models import OrderbookTick

logger = logging.getLogger(__name__)

# Parquet schema — int16 for points (0-100), float32 for sizes
TICK_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("market_id", pa.string()),
    ("crypto_asset", pa.string()),
    ("time_remaining", pa.float32()),
    ("yes_best_bid", pa.int16()),
    ("yes_best_ask", pa.int16()),
    ("no_best_bid", pa.int16()),
    ("no_best_ask", pa.int16()),
    ("yes_bid_size", pa.float32()),
    ("yes_ask_size", pa.float32()),
    ("no_bid_size", pa.float32()),
    ("no_ask_size", pa.float32()),
])

# Max ticks per buffer before oldest are dropped (~30 min at 2s interval)
MAX_BUFFER_SIZE = 900


class TickBuffer:
    """Per-market bounded deque of OrderbookTick objects."""

    def __init__(self, maxlen: int = MAX_BUFFER_SIZE):
        self._ticks: deque[OrderbookTick] = deque(maxlen=maxlen)

    def append(self, tick: OrderbookTick) -> None:
        self._ticks.append(tick)

    def get_recent(self, n: int) -> list[OrderbookTick]:
        """Return the last N ticks (for live momentum computation)."""
        return list(self._ticks)[-n:]

    def drain(self) -> list[OrderbookTick]:
        """Return all ticks and clear the buffer."""
        ticks = list(self._ticks)
        self._ticks.clear()
        return ticks

    def __len__(self) -> int:
        return len(self._ticks)


class TickStore:
    """Manages per-market tick buffers with periodic S3 persistence."""

    def __init__(self, config: DataConfig):
        self._config = config
        self._buffers: dict[str, TickBuffer] = defaultdict(TickBuffer)
        self._flush_task: Optional[asyncio.Task] = None
        self._s3_client = None

    def _get_s3_client(self):
        """Lazy-init boto3 S3 client."""
        if self._s3_client is None:
            import boto3
            self._s3_client = boto3.client("s3")
        return self._s3_client

    def record_tick(self, tick: OrderbookTick) -> None:
        """Route a tick to the correct market buffer."""
        self._buffers[tick.market_id].append(tick)

    def get_buffer(self, market_id: str) -> TickBuffer:
        """Return the buffer for a market (for live reads)."""
        return self._buffers[market_id]

    def start_periodic_flush(self) -> None:
        """Start background task that flushes all buffers periodically."""
        if self._flush_task is not None:
            return
        self._flush_task = asyncio.create_task(self._periodic_flush_loop())
        logger.info("Tick store: periodic flush started (interval=%.0fs)",
                     self._config.tick_flush_interval_seconds)

    async def stop(self) -> None:
        """Flush remaining data and cancel background task."""
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        # Final flush of all buffers
        for market_id in list(self._buffers.keys()):
            await self.flush_market(market_id)
        logger.info("Tick store stopped — all buffers flushed")

    async def flush_market(self, market_id: str, *, remove_after: bool = False) -> None:
        """Flush one market's buffer: write Parquet → upload to S3 → delete tmp.

        Args:
            remove_after: If True, remove the buffer entry entirely after flush.
                          Used at market settlement to prevent unbounded dict growth.
        """
        buf = self._buffers.get(market_id)
        if buf is None or len(buf) == 0:
            if remove_after:
                self._buffers.pop(market_id, None)
            return

        ticks = buf.drain()
        if not ticks:
            if remove_after:
                self._buffers.pop(market_id, None)
            return

        bucket = self._config.tick_s3_bucket
        if not bucket:
            logger.debug("Tick flush skipped — no S3 bucket configured")
            if remove_after:
                self._buffers.pop(market_id, None)
            return

        try:
            await asyncio.to_thread(self._flush_sync, ticks, bucket)
        except Exception as e:
            logger.error("Tick flush failed for %s: %s", market_id, e)
        finally:
            if remove_after:
                self._buffers.pop(market_id, None)

    def _flush_sync(self, ticks: list[OrderbookTick], bucket: str) -> None:
        """Synchronous: build table → write tmp Parquet → upload to S3."""
        if not ticks:
            return

        first = ticks[0]
        date_str = first.timestamp.strftime("%Y-%m-%d")
        crypto = first.crypto_asset
        # Derive slug from market_id
        slug = first.market_id

        s3_key = (
            f"{self._config.tick_s3_prefix}/asset={crypto}/"
            f"date={date_str}/{slug}.parquet"
        )

        # Build pyarrow table
        table = self._ticks_to_table(ticks)

        # Check if existing file on S3 — if so, download + concat
        s3 = self._get_s3_client()
        existing_table = self._download_existing(s3, bucket, s3_key)
        if existing_table is not None:
            table = pa.concat_tables([existing_table, table])

        # Write to temp file and upload
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
            pq.write_table(table, tmp.name, compression="snappy")
            s3.upload_file(tmp.name, bucket, s3_key)

        logger.debug("Flushed %d ticks to s3://%s/%s", len(ticks), bucket, s3_key)

    def _download_existing(self, s3, bucket: str, key: str) -> Optional[pa.Table]:
        """Download existing Parquet from S3, return None if not found."""
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
                s3.download_file(bucket, key, tmp.name)
                return pq.read_table(tmp.name)
        except s3.exceptions.ClientError:
            return None
        except Exception:
            return None

    @staticmethod
    def _ticks_to_table(ticks: list[OrderbookTick]) -> pa.Table:
        """Convert a list of ticks to a pyarrow Table."""
        return pa.table({
            "timestamp": [t.timestamp for t in ticks],
            "market_id": [t.market_id for t in ticks],
            "crypto_asset": [t.crypto_asset for t in ticks],
            "time_remaining": [t.time_remaining for t in ticks],
            "yes_best_bid": [t.yes_best_bid for t in ticks],
            "yes_best_ask": [t.yes_best_ask for t in ticks],
            "no_best_bid": [t.no_best_bid for t in ticks],
            "no_best_ask": [t.no_best_ask for t in ticks],
            "yes_bid_size": [t.yes_bid_size for t in ticks],
            "yes_ask_size": [t.yes_ask_size for t in ticks],
            "no_bid_size": [t.no_bid_size for t in ticks],
            "no_ask_size": [t.no_ask_size for t in ticks],
        }, schema=TICK_SCHEMA)

    async def cleanup_old_files(self) -> None:
        """Delete S3 objects with date prefix older than retention threshold."""
        bucket = self._config.tick_s3_bucket
        if not bucket:
            return

        try:
            await asyncio.to_thread(self._cleanup_sync, bucket)
        except Exception as e:
            logger.error("Tick retention cleanup failed: %s", e)

    def _cleanup_sync(self, bucket: str) -> None:
        """Synchronous: list and delete old S3 objects."""
        s3 = self._get_s3_client()
        prefix = self._config.tick_s3_prefix + "/"
        cutoff = datetime.now(timezone.utc).date() - timedelta(
            days=self._config.tick_retention_days
        )

        paginator = s3.get_paginator("list_objects_v2")
        to_delete: list[str] = []

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                # Parse date from hive partition: .../date=YYYY-MM-DD/...
                date_part = self._extract_date_from_key(key)
                if date_part and date_part < cutoff:
                    to_delete.append(key)

        if not to_delete:
            return

        # Delete in batches of 1000 (S3 limit)
        for i in range(0, len(to_delete), 1000):
            batch = to_delete[i:i + 1000]
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in batch]},
            )

        logger.info("Tick retention: deleted %d old objects from s3://%s/%s",
                     len(to_delete), bucket, prefix)

    @staticmethod
    def _extract_date_from_key(key: str) -> Optional[date]:
        """Extract date from hive partition path like .../date=2026-03-18/..."""
        import re
        m = re.search(r"date=(\d{4}-\d{2}-\d{2})", key)
        if m:
            return datetime.strptime(m.group(1), "%Y-%m-%d").date()
        return None

    async def _periodic_flush_loop(self) -> None:
        """Background loop: flush all buffers at the configured interval."""
        interval = self._config.tick_flush_interval_seconds
        while True:
            await asyncio.sleep(interval)
            for market_id in list(self._buffers.keys()):
                await self.flush_market(market_id)
