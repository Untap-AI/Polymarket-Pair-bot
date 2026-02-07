"""Market monitor: orchestrates WebSocket + scheduled cycles + evaluator(s).

Manages the full lifecycle of ONE Polymarket 15-minute crypto market:
  1. Connect WebSocket and subscribe to both token orderbooks
  2. Wait for initial data
  3. Run measurement cycles on a fixed schedule
  4. At each cycle: snapshot → trigger evaluation (per param set) → DB persistence
  5. On settlement (or shutdown): finalize all attempts, write summary
"""

from __future__ import annotations

import asyncio
import logging
import statistics
import time as _time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .config import AppConfig
from .database import Database
from .models import (
    MarketInfo,
    ParameterSet,
    SamplingMode,
    Snapshot,
)
from .rest_client import CLOBRestClient
from .trigger_evaluator import TriggerEvaluator
from .websocket_client import WebSocketClient

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Summary dataclass
# ---------------------------------------------------------------------------

@dataclass
class MarketSummary:
    """Final summary for a monitored market session."""
    market_id: str
    crypto_asset: str
    total_attempts: int
    total_pairs: int
    total_failed: int
    settlement_failures: int
    pair_rate: float
    avg_time_to_pair: Optional[float]
    median_time_to_pair: Optional[float]
    max_concurrent: int
    total_cycles: int
    cycle_interval: float
    time_remaining_at_start: float
    anomaly_count: int


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class MarketMonitor:
    """Orchestrates monitoring of one market from connection to settlement.

    Supports multiple parameter sets: one ``TriggerEvaluator`` per param set,
    each independently tracking attempts.  The *primary* evaluator (first
    param set) is used for status display and the returned ``MarketSummary``.
    """

    def __init__(
        self,
        market_info: MarketInfo,
        params_list: list[ParameterSet],
        config: AppConfig,
        database: Database,
        ws_client: WebSocketClient,
        rest_client: CLOBRestClient,
        shutdown_event: Optional[asyncio.Event] = None,
        event_log: Optional[deque] = None,
    ):
        self.market_info = market_info
        self.params_list = params_list
        self.config = config
        self.db = database
        self.ws = ws_client
        self.rest = rest_client
        self._shutdown_event = shutdown_event
        self._event_log = event_log

        # One evaluator per parameter set
        self._evaluators: dict[int, TriggerEvaluator] = {}
        for ps in params_list:
            ps_id = ps.parameter_set_id or 0
            self._evaluators[ps_id] = TriggerEvaluator(
                params=ps,
                market_info=market_info,
                max_ref_sum_deviation=config.quality.max_reference_sum_deviation,
                enable_lifecycle=config.data.enable_lifecycle_tracking,
            )
        self._primary_ps_id = (params_list[0].parameter_set_id or 0)
        self._pair_times: dict[int, list[float]] = {
            (ps.parameter_set_id or 0): [] for ps in params_list
        }

        # Cycle scheduling
        self.cycle_interval: float = 0.0
        self.total_planned_cycles: int = 0
        self.cycles_run: int = 0

        # State
        self.start_time: Optional[datetime] = None
        self.time_remaining_at_start: float = 0.0
        self.anomaly_count: int = 0
        self._was_shutdown: bool = False

    @property
    def evaluator(self) -> TriggerEvaluator:
        """Primary evaluator (first param set) — used for status display."""
        return self._evaluators[self._primary_ps_id]

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------

    def _push_event(self, msg: str) -> None:
        if self._event_log is not None:
            self._event_log.append((
                datetime.now(timezone.utc),
                self.market_info.crypto_asset.upper(),
                msg,
            ))

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def run(self) -> MarketSummary:
        """Run the full monitoring lifecycle and return a summary."""
        now = datetime.now(timezone.utc)
        settlement = self.market_info.settlement_time
        self.time_remaining_at_start = (settlement - now).total_seconds()

        if self.time_remaining_at_start <= 0:
            logger.warning("Market %s already settled!", self.market_info.market_slug)
            return self._build_summary()

        self.start_time = now
        logger.info(
            "Starting monitor for %s — %.0fs remaining until settlement at %s",
            self.market_info.market_slug,
            self.time_remaining_at_start,
            settlement.strftime("%H:%M:%S UTC"),
        )

        # Calculate cycle schedule
        self._calculate_schedule()

        # Connect WebSocket
        try:
            await self.ws.connect_and_subscribe([
                self.market_info.yes_token_id,
                self.market_info.no_token_id,
            ])
        except Exception as e:
            logger.error("Failed to connect WS for %s: %s",
                         self.market_info.market_slug, e)
            return self._build_summary()

        # Wait for initial orderbook data from WS
        await self._wait_for_initial_data()

        # Insert market record in DB (primary param set)
        await self.db.insert_market(
            self.market_info,
            self._primary_ps_id,
            self.start_time,
            self.time_remaining_at_start,
            self.cycle_interval,
        )

        # Run measurement cycles until settlement or shutdown
        try:
            await self._run_cycles()
        except asyncio.CancelledError:
            self._was_shutdown = True
            logger.info("Monitoring cancelled for %s", self.market_info.market_slug)
        except Exception as e:
            logger.error("Error during cycle execution: %s", e, exc_info=True)

        # Process settlement — fail all remaining active attempts
        fail_reason = "bot_shutdown" if self._was_shutdown else "settlement_reached"
        await self._process_settlement(fail_reason)

        # Stop WebSocket
        await self.ws.stop()

        # Build and persist summary (primary param set)
        summary = self._build_summary()
        await self._write_summary(summary)

        # Log summaries for non-primary param sets
        for ps_id, ev in self._evaluators.items():
            if ps_id != self._primary_ps_id and ev.total_attempts > 0:
                logger.info(
                    "[%s] Param '%s': %d attempts, %d pairs (%.0f%%)",
                    self.market_info.crypto_asset.upper(),
                    ev.params.name,
                    ev.total_attempts,
                    ev.total_pairs,
                    ev.total_pairs / max(1, ev.total_attempts) * 100,
                )

        return summary

    # ------------------------------------------------------------------
    # Scheduling
    # ------------------------------------------------------------------

    def _calculate_schedule(self) -> None:
        """Determine cycle interval and planned count."""
        sampling = self.config.sampling

        if sampling.mode == SamplingMode.FIXED_INTERVAL:
            self.cycle_interval = sampling.cycle_interval_seconds
            self.total_planned_cycles = max(
                1, int(self.time_remaining_at_start / self.cycle_interval)
            )
        else:  # FIXED_COUNT
            self.total_planned_cycles = sampling.cycles_per_market
            self.cycle_interval = max(
                1.0, self.time_remaining_at_start / self.total_planned_cycles
            )

        logger.info(
            "Cycle schedule: interval=%.1fs, planned_cycles=%d (mode=%s, remaining=%.0fs)",
            self.cycle_interval,
            self.total_planned_cycles,
            sampling.mode.value,
            self.time_remaining_at_start,
        )

    # ------------------------------------------------------------------
    # Interruptible sleep
    # ------------------------------------------------------------------

    async def _interruptible_sleep(self, duration: float) -> bool:
        """Sleep for *duration*, returning ``True`` if shutdown was requested."""
        if self._shutdown_event is None:
            await asyncio.sleep(duration)
            return False
        try:
            await asyncio.wait_for(
                asyncio.shield(self._shutdown_event.wait()), timeout=duration
            )
            return True
        except asyncio.TimeoutError:
            return False

    # ------------------------------------------------------------------
    # Data readiness
    # ------------------------------------------------------------------

    async def _wait_for_initial_data(self, timeout: float = 15.0) -> None:
        """Block until both YES and NO orderbooks have bid+ask data."""
        start = asyncio.get_event_loop().time()

        while (asyncio.get_event_loop().time() - start) < timeout:
            yes_ob = self.ws.get_orderbook(self.market_info.yes_token_id)
            no_ob = self.ws.get_orderbook(self.market_info.no_token_id)

            if (
                yes_ob and yes_ob.best_bid is not None and yes_ob.best_ask is not None
                and no_ob and no_ob.best_bid is not None and no_ob.best_ask is not None
            ):
                logger.info(
                    "Initial orderbook for %s — YES: bid=%d ask=%d, NO: bid=%d ask=%d",
                    self.market_info.market_slug,
                    yes_ob.best_bid, yes_ob.best_ask,
                    no_ob.best_bid, no_ob.best_ask,
                )
                return

            await asyncio.sleep(0.5)

        logger.warning(
            "Timeout (%.0fs) waiting for orderbook data for %s",
            timeout, self.market_info.market_slug,
        )

    # ------------------------------------------------------------------
    # Cycle execution
    # ------------------------------------------------------------------

    async def _run_cycles(self) -> None:
        """Execute measurement cycles on schedule until settlement or shutdown."""
        settlement = self.market_info.settlement_time

        # Run the first cycle immediately
        await self._execute_cycle()

        while True:
            if await self._interruptible_sleep(self.cycle_interval):
                self._was_shutdown = True
                logger.info("Shutdown during cycles for %s",
                            self.market_info.market_slug)
                return

            now = datetime.now(timezone.utc)
            time_remaining = (settlement - now).total_seconds()

            if time_remaining <= 0:
                logger.info("Settlement time reached for %s",
                            self.market_info.market_slug)
                break

            if self._detect_feed_gap():
                logger.warning(
                    "Feed gap detected — skipping cycle %d for %s",
                    self.cycles_run + 1, self.market_info.market_slug,
                )
                for ev in self._evaluators.values():
                    ev.mark_feed_gap()
                continue

            await self._execute_cycle()

    async def _execute_cycle(self) -> None:
        """Run one measurement cycle across all parameter sets.

        Collects all DB writes across evaluators and flushes them in
        batches — one round-trip per operation type instead of one per row.
        """
        self.cycles_run += 1
        now = datetime.now(timezone.utc)
        time_remaining = (self.market_info.settlement_time - now).total_seconds()

        # Capture a single snapshot (shared by all evaluators)
        snapshot = self._capture_snapshot(self.cycles_run, now, time_remaining)

        # Log prices every 10th cycle or first cycle for debugging
        if self.cycles_run <= 3 or self.cycles_run % 10 == 0:
            logger.info(
                "[%s] Cycle %d prices — YES: bid=%s ask=%s, NO: bid=%s ask=%s",
                self.market_info.crypto_asset.upper(), self.cycles_run,
                snapshot.yes_bid_points, snapshot.yes_ask_points,
                snapshot.no_bid_points, snapshot.no_ask_points,
            )

        # --- Evaluate all param sets (pure compute, no I/O) ---
        all_new_attempts: list = []
        all_paired_attempts: list = []
        all_lifecycle_records: list = []
        has_activity = False
        primary_active_count = 0
        primary_anomaly = False

        for ps_id, evaluator in self._evaluators.items():
            result = evaluator.evaluate_cycle(
                snapshot=snapshot,
                cycle_number=self.cycles_run,
                cycle_time=now,
                time_remaining=time_remaining,
            )

            if result.anomaly:
                self.anomaly_count += 1

            # Collect new attempts
            all_new_attempts.extend(result.new_attempts)

            # Collect paired attempts + bookkeeping
            for attempt in result.paired_attempts:
                if attempt.time_to_pair_seconds is not None:
                    self._pair_times[ps_id].append(attempt.time_to_pair_seconds)
            all_paired_attempts.extend(result.paired_attempts)

            # Collect lifecycle records
            all_lifecycle_records.extend(result.lifecycle_records)

            # Track primary param set state for snapshot/events
            if ps_id == self._primary_ps_id:
                primary_active_count = result.active_count
                primary_anomaly = result.anomaly
                if result.new_attempts or result.paired_attempts:
                    has_activity = True

        # --- Batch DB writes (minimal round-trips) ---
        if all_new_attempts:
            await self.db.insert_attempts_batch(all_new_attempts)
            # Push events for primary param set (IDs now assigned)
            for attempt in all_new_attempts:
                if attempt.parameter_set_id == self._primary_ps_id:
                    self._push_event(
                        f"Attempt #{attempt.attempt_id} started "
                        f"({attempt.first_leg_side.value} first "
                        f"@ {attempt.P1_points}pts)"
                    )

        if all_paired_attempts:
            await self.db.update_attempts_paired_batch(all_paired_attempts)
            for attempt in all_paired_attempts:
                if attempt.parameter_set_id == self._primary_ps_id:
                    self._push_event(
                        f"Attempt #{attempt.attempt_id} PAIRED in "
                        f"{attempt.time_to_pair_seconds:.1f}s "
                        f"(cost: {attempt.pair_cost_points}, "
                        f"profit: {attempt.pair_profit_points})"
                    )

        if all_lifecycle_records:
            await self.db.insert_lifecycle_batch(all_lifecycle_records)

        if self.config.data.enable_snapshots:
            snapshot.active_attempts_count = primary_active_count
            snapshot.anomaly_flag = primary_anomaly
            await self.db.insert_snapshot(snapshot)

        # Log cycle summary (primary evaluator)
        if has_activity:
            ev = self.evaluator
            total_att = ev.total_attempts
            total_pair = ev.total_pairs
            pct = (total_pair / max(1, total_att)) * 100
            logger.info(
                "[%s] Cycle %d/%d: %d active | "
                "%d attempts, %d pairs (%.0f%%) | %.0fs left",
                self.market_info.crypto_asset.upper(),
                self.cycles_run, self.total_planned_cycles,
                len(ev.active_attempts),
                total_att, total_pair, pct,
                time_remaining,
            )

    # ------------------------------------------------------------------
    # Snapshot capture
    # ------------------------------------------------------------------

    def _capture_snapshot(
        self, cycle_number: int, timestamp: datetime, time_remaining: float
    ) -> Snapshot:
        yes_ob = self.ws.get_orderbook(self.market_info.yes_token_id)
        no_ob = self.ws.get_orderbook(self.market_info.no_token_id)

        return Snapshot(
            market_id=self.market_info.market_slug,
            cycle_number=cycle_number,
            timestamp=timestamp,
            yes_bid_points=yes_ob.best_bid if yes_ob else None,
            yes_ask_points=yes_ob.best_ask if yes_ob else None,
            no_bid_points=no_ob.best_bid if no_ob else None,
            no_ask_points=no_ob.best_ask if no_ob else None,
            yes_last_trade_points=yes_ob.last_trade_price if yes_ob else None,
            no_last_trade_points=no_ob.last_trade_price if no_ob else None,
            time_remaining_seconds=time_remaining,
        )

    # ------------------------------------------------------------------
    # Feed gap detection
    # ------------------------------------------------------------------

    def _detect_feed_gap(self) -> bool:
        last_msg = self.ws.last_message_time
        if last_msg is None:
            return True
        gap = _time.time() - last_msg
        return gap > self.config.quality.feed_gap_threshold_seconds

    # ------------------------------------------------------------------
    # Settlement
    # ------------------------------------------------------------------

    async def _process_settlement(self, fail_reason: str = "settlement_reached") -> None:
        """Fail all remaining active attempts across all evaluators (batched)."""
        now = datetime.now(timezone.utc)
        all_failed: list = []
        for ps_id, evaluator in self._evaluators.items():
            failed = evaluator.process_settlement(now, fail_reason=fail_reason)
            all_failed.extend(failed)
            if failed:
                logger.info(
                    "Settlement for %s (ps=%s): %d attempt(s) finalized (reason=%s)",
                    self.market_info.market_slug, evaluator.params.name,
                    len(failed), fail_reason,
                )
        if all_failed:
            await self.db.update_attempts_failed_batch(all_failed)

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def _build_summary(self) -> MarketSummary:
        """Build summary from the primary evaluator."""
        ev = self.evaluator
        total_att = ev.total_attempts
        total_pairs = ev.total_pairs
        total_failed = ev.total_failed
        pair_rate = total_pairs / max(1, total_att)

        times = self._pair_times.get(self._primary_ps_id, [])
        avg_ttp: Optional[float] = None
        median_ttp: Optional[float] = None
        if times:
            avg_ttp = sum(times) / len(times)
            median_ttp = statistics.median(times)

        return MarketSummary(
            market_id=self.market_info.market_slug,
            crypto_asset=self.market_info.crypto_asset,
            total_attempts=total_att,
            total_pairs=total_pairs,
            total_failed=total_failed,
            settlement_failures=total_failed,
            pair_rate=pair_rate,
            avg_time_to_pair=avg_ttp,
            median_time_to_pair=median_ttp,
            max_concurrent=ev.max_concurrent,
            total_cycles=self.cycles_run,
            cycle_interval=self.cycle_interval,
            time_remaining_at_start=self.time_remaining_at_start,
            anomaly_count=self.anomaly_count,
        )

    async def _write_summary(self, summary: MarketSummary) -> None:
        await self.db.update_market_summary(
            market_id=summary.market_id,
            total_attempts=summary.total_attempts,
            total_pairs=summary.total_pairs,
            total_failed=summary.total_failed,
            settlement_failures=summary.settlement_failures,
            pair_rate=summary.pair_rate,
            avg_time_to_pair=summary.avg_time_to_pair,
            median_time_to_pair=summary.median_time_to_pair,
            max_concurrent=summary.max_concurrent,
            total_cycles=summary.total_cycles,
            anomaly_count=summary.anomaly_count,
        )
