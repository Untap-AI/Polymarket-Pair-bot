"""Core trigger evaluation engine.

Implements the heart of the measurement bot:
  • Reference price calculation (midpoint)
  • Pending limit order placement and refresh (maker simulation)
  • First-leg fill detection (period_low_ask vs limit price)
  • Attempt creation with pair-constraint enforcement
  • Active attempt monitoring and pairing
  • Closest-approach tracking (always on)
  • Per-cycle lifecycle records (optional, high-volume)
  • Simultaneous fill handling with tie-breaking
  • Taker-risk tracking (placement buffer, cycles to fill)
  • Settlement processing
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .models import (
    Attempt,
    AttemptStatus,
    LifecycleRecord,
    MarketInfo,
    ParameterSet,
    PendingLimit,
    Side,
    Snapshot,
)
from .price_utils import clamp_trigger, midpoint_points, round_to_tick

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cycle result
# ---------------------------------------------------------------------------

@dataclass
class CycleResult:
    """Everything that happened during one measurement cycle."""
    new_attempts: list[Attempt] = field(default_factory=list)
    paired_attempts: list[Attempt] = field(default_factory=list)
    active_count: int = 0
    pending_limit_count: int = 0
    skipped: bool = False
    skip_reason: str = ""
    anomaly: bool = False
    anomaly_detail: str = ""
    lifecycle_records: list[LifecycleRecord] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Evaluator
# ---------------------------------------------------------------------------

class TriggerEvaluator:
    """Stateful engine that evaluates triggers and manages attempt lifecycle.

    Holds pending limits, active attempts, and running counters.  The
    evaluator is pure compute — no I/O, no async.  The ``MarketMonitor``
    feeds it snapshots and persists the returned results.

    Both legs use maker-style limit orders:
      • First leg: a pending limit is placed/refreshed each cycle at
        ``min(reference - S0, ask - maker_buffer)``.  Fills when
        ``period_low_ask <= limit_price``.
      • Second leg: a fixed opposite trigger is set at attempt creation
        and monitored until ``period_low_ask <= opposite_trigger``.
    """

    def __init__(
        self,
        params: ParameterSet,
        market_info: MarketInfo,
        max_ref_sum_deviation: int = 2,
        enable_lifecycle: bool = False,
    ):
        self.params = params
        self.market_info = market_info
        self.max_ref_sum_deviation = max_ref_sum_deviation
        self.enable_lifecycle = enable_lifecycle
        self.tick = market_info.tick_size_points

        # Pending first-leg limits (at most one per side)
        self._pending_limits: dict[Side, PendingLimit] = {}

        # Attempt tracking
        self.active_attempts: list[Attempt] = []
        self._attempt_counter: int = 0  # local counter (DB assigns real IDs)

        # Per-attempt rolling trackers (key = id(attempt object))
        self._closest_approach: dict[int, int] = {}
        self._closest_approach_ts: dict[int, datetime] = {}
        self._closest_approach_cn: dict[int, int] = {}
        self._mae: dict[int, int] = {}           # max adverse excursion
        self._mae_ts: dict[int, datetime] = {}
        self._mae_cn: dict[int, int] = {}

        # Running statistics
        self.total_attempts: int = 0
        self.total_pairs: int = 0
        self.total_failed: int = 0
        self.max_concurrent: int = 0

    # ------------------------------------------------------------------
    # Main evaluation entry point
    # ------------------------------------------------------------------

    def evaluate_cycle(
        self,
        snapshot: Snapshot,
        cycle_number: int,
        cycle_time: datetime,
        time_remaining: float,
    ) -> CycleResult:
        """Evaluate pending limits and active attempts at a measurement cycle.

        Processing steps (order matters for period_low_ask correctness):
          1. Validate orderbook
          2. Calculate reference prices (current bid/ask midpoints)
          3. Check existing pending limit fills (period_low_ask vs old limits)
          4. Refresh remaining pending limits (current prices, forward-looking)
          5. Same-cycle fill check (period_low_ask vs newly placed limits)
          6. Update all active attempts (check for pairing)
          7. Closest approach + MAE + lifecycle tracking
          8. Clean up completed
        """
        result = CycleResult()

        # --- Step 1: validate orderbook data ---
        if not self._has_valid_orderbook(snapshot):
            result.skipped = True
            result.skip_reason = "orderbook_empty"
            logger.warning("Cycle %d: skipped — incomplete orderbook", cycle_number)
            result.active_count = len(self.active_attempts)
            result.pending_limit_count = len(self._pending_limits)
            return result

        # --- Step 2: reference prices (midpoints from current bid/ask) ---
        yes_ref = midpoint_points(snapshot.yes_bid_points, snapshot.yes_ask_points)
        no_ref = midpoint_points(snapshot.no_bid_points, snapshot.no_ask_points)

        # Sanity check: sum should be ~100
        ref_sum = yes_ref + no_ref
        if abs(ref_sum - 100) > self.max_ref_sum_deviation:
            result.anomaly = True
            result.anomaly_detail = (
                f"reference_sum_anomaly: {ref_sum:.1f} (expected ~100)"
            )
            logger.warning("Cycle %d: %s", cycle_number, result.anomaly_detail)

        ref_yes_int = int(yes_ref)
        ref_no_int = int(no_ref)
        pair_cap = self.params.pair_cap_points

        # Period-low asks (fall back to instantaneous if unavailable)
        yes_low_ask = snapshot.yes_period_low_ask_points or snapshot.yes_ask_points
        no_low_ask = snapshot.no_period_low_ask_points or snapshot.no_ask_points

        # Remember pre-existing attempts (they have valid DB IDs)
        pre_existing_ids = set(id(a) for a in self.active_attempts)

        # --- Step 3: check existing pending limit fills ---
        #
        # Check limits carried from a PREVIOUS cycle against period_low_ask.
        # These are true maker fills — the price came down to our posted
        # limit during the inter-cycle window.
        #
        new_attempts: list[Attempt] = []
        filled_sides: list[Side] = []

        for side, pending in list(self._pending_limits.items()):
            low_ask = yes_low_ask if side == Side.YES else no_low_ask
            if low_ask is not None and low_ask <= pending.limit_price_points:
                cycles_waited = cycle_number - pending.placed_cycle
                if cycles_waited >= 1:
                    # True delayed fill — price came to our limit
                    attempt = self._create_attempt_from_pending(
                        pending, ref_yes_int, ref_no_int,
                        cycle_number, cycle_time, time_remaining, snapshot,
                        cycles_to_fill=cycles_waited,
                    )
                    new_attempts.append(attempt)
                    filled_sides.append(side)
                    logger.info(
                        "Cycle %d: %s limit FILLED (delayed, %d cycles) — "
                        "limit=%d, low_ask=%d, buffer=%d",
                        cycle_number, side.value, cycles_waited,
                        pending.limit_price_points, low_ask,
                        pending.ask_at_placement_points - pending.limit_price_points,
                    )

        # Remove filled limits
        for side in filled_sides:
            self._pending_limits.pop(side, None)

        # --- Step 4: refresh remaining pending limits ---
        #
        # For each side without a pending limit (or with an unfilled one),
        # compute a new limit price based on CURRENT market conditions.
        # Uses current ask (not period_low_ask) since this is forward-looking.
        #
        for side in (Side.YES, Side.NO):
            if side in filled_sides:
                # Just filled in Step 3 — don't immediately re-place
                continue

            current_ask = (
                snapshot.yes_ask_points if side == Side.YES
                else snapshot.no_ask_points
            )
            ref = yes_ref if side == Side.YES else no_ref

            # Limit price = min(reference - S0, ask - maker_buffer)
            limit_from_ref = round_to_tick(ref - self.params.S0_points, self.tick)
            limit_from_buffer = round_to_tick(
                current_ask - self.params.maker_buffer_points, self.tick
            )
            limit_price = min(limit_from_ref, limit_from_buffer)
            limit_price = clamp_trigger(limit_price, self.tick)

            # PairCap guard: don't place if limit >= PairCap (impossible pair)
            if limit_price >= pair_cap:
                # Remove any stale pending limit
                self._pending_limits.pop(side, None)
                logger.debug(
                    "Cycle %d: %s limit suppressed — limit=%d >= PairCap=%d",
                    cycle_number, side.value, limit_price, pair_cap,
                )
                continue

            self._pending_limits[side] = PendingLimit(
                side=side,
                limit_price_points=limit_price,
                ask_at_placement_points=current_ask,
                reference_yes_points=ref_yes_int,
                reference_no_points=ref_no_int,
                placed_cycle=cycle_number,
                placed_timestamp=cycle_time,
            )

        # --- Step 5: same-cycle fill check ---
        #
        # Check if limits just placed/refreshed in Step 4 would have
        # filled against the period_low_ask.  This catches the case where
        # the market already visited our limit level during the inter-cycle
        # window.  Flagged as cycles_to_fill=0 (highest taker risk).
        #
        same_cycle_filled: list[Side] = []

        for side, pending in list(self._pending_limits.items()):
            low_ask = yes_low_ask if side == Side.YES else no_low_ask
            if low_ask is not None and low_ask <= pending.limit_price_points:
                attempt = self._create_attempt_from_pending(
                    pending, ref_yes_int, ref_no_int,
                    cycle_number, cycle_time, time_remaining, snapshot,
                    cycles_to_fill=0,
                )
                new_attempts.append(attempt)
                same_cycle_filled.append(side)
                logger.info(
                    "Cycle %d: %s limit FILLED (same-cycle) — "
                    "limit=%d, low_ask=%d, buffer=%d",
                    cycle_number, side.value,
                    pending.limit_price_points, low_ask,
                    pending.ask_at_placement_points - pending.limit_price_points,
                )

        for side in same_cycle_filled:
            self._pending_limits.pop(side, None)

        # Handle simultaneous fills (both YES and NO filled this cycle)
        if len(new_attempts) == 2:
            # Tie-break: side with larger distance below its limit (touched harder)
            a0, a1 = new_attempts
            ask0 = yes_low_ask if a0.first_leg_side == Side.YES else no_low_ask
            ask1 = yes_low_ask if a1.first_leg_side == Side.YES else no_low_ask
            dist0 = a0.P1_points - ask0 if ask0 else 0
            dist1 = a1.P1_points - ask1 if ask1 else 0

            if dist0 < dist1:
                # a1 touched harder — put it first
                new_attempts = [a1, a0]
            # else: a0 already first (touched harder, or tie → YES fallback
            # since YES is checked/created first)

            logger.info(
                "Cycle %d: SIMULTANEOUS fill — %s first (dist=%d), %s second (dist=%d)",
                cycle_number,
                new_attempts[0].first_leg_side.value, max(dist0, dist1),
                new_attempts[1].first_leg_side.value, min(dist0, dist1),
            )

        # Add new attempts to the active list
        self.active_attempts.extend(new_attempts)
        result.new_attempts = new_attempts

        # --- Step 6: update ALL active attempts (check for pairing) ---
        paired: list[Attempt] = []
        still_active: list[Attempt] = []

        for attempt in self.active_attempts:
            # Use period-low ask for pairing check: the lowest ask seen
            # during this inter-cycle window.  This catches fills that
            # happened between 10-second cycles but bounced back.
            opp_ask = (
                (snapshot.yes_period_low_ask_points or snapshot.yes_ask_points)
                if attempt.opposite_side == Side.YES
                else (snapshot.no_period_low_ask_points or snapshot.no_ask_points)
            )

            if opp_ask is not None and opp_ask <= attempt.opposite_trigger_points:
                # *** PAIRED ***
                # When simulating limit orders, the fill price is the limit order price
                # (opposite_trigger_points), not the current ask. This ensures profit is
                # fixed at delta as intended.
                limit_fill_price = attempt.opposite_trigger_points
                attempt.status = AttemptStatus.COMPLETED_PAIRED
                attempt.t2_timestamp = cycle_time
                attempt.t2_cycle_number = cycle_number
                attempt.time_to_pair_seconds = (
                    (cycle_time - attempt.t1_timestamp).total_seconds()
                )
                attempt.actual_opposite_price = limit_fill_price
                attempt.pair_cost_points = attempt.P1_points + limit_fill_price
                attempt.pair_profit_points = 100 - attempt.pair_cost_points
                attempt.time_remaining_at_completion = time_remaining

                # Closest approach = 0 (touched/crossed)
                attempt.closest_approach_points = 0
                attempt.closest_approach_timestamp = cycle_time
                attempt.closest_approach_cycle_number = cycle_number

                # Finalize MAE from tracker
                key = id(attempt)
                if key in self._mae:
                    attempt.max_adverse_excursion_points = self._mae[key]
                    attempt.mae_timestamp = self._mae_ts.get(key)
                    attempt.mae_cycle_number = self._mae_cn.get(key)
                else:
                    attempt.max_adverse_excursion_points = 0

                # Exit spreads (Feature 5)
                if snapshot.yes_ask_points is not None and snapshot.yes_bid_points is not None:
                    attempt.yes_spread_exit_points = snapshot.yes_ask_points - snapshot.yes_bid_points
                if snapshot.no_ask_points is not None and snapshot.no_bid_points is not None:
                    attempt.no_spread_exit_points = snapshot.no_ask_points - snapshot.no_bid_points

                self.total_pairs += 1
                # Clean up trackers
                self._closest_approach.pop(key, None)
                self._closest_approach_ts.pop(key, None)
                self._closest_approach_cn.pop(key, None)
                self._mae.pop(key, None)
                self._mae_ts.pop(key, None)
                self._mae_cn.pop(key, None)
                paired.append(attempt)

                logger.info(
                    "Cycle %d: PAIRED attempt #%d — %s-first, cost=%dpt, "
                    "profit=%dpt, time=%.1fs",
                    cycle_number,
                    attempt.attempt_id,
                    attempt.first_leg_side.value,
                    attempt.pair_cost_points,
                    attempt.pair_profit_points,
                    attempt.time_to_pair_seconds,
                )
            else:
                still_active.append(attempt)

        # --- Step 7: clean up completed ---
        self.active_attempts = still_active
        result.paired_attempts = paired

        # --- Step 8: closest approach + MAE + lifecycle for remaining active ---
        for attempt in self.active_attempts:
            key = id(attempt)

            # -- Feature 1: closest approach to opposite trigger --
            # Use period-low ask for more accurate closest-approach tracking.
            opp_ask = (
                (snapshot.yes_period_low_ask_points or snapshot.yes_ask_points)
                if attempt.opposite_side == Side.YES
                else (snapshot.no_period_low_ask_points or snapshot.no_ask_points)
            )
            if opp_ask is not None and opp_ask > 0:
                dist = opp_ask - attempt.opposite_trigger_points
                prev = self._closest_approach.get(key, 9999)
                if dist < prev:
                    self._closest_approach[key] = dist
                    self._closest_approach_ts[key] = cycle_time
                    self._closest_approach_cn[key] = cycle_number
                attempt.closest_approach_points = self._closest_approach[key]
                attempt.closest_approach_timestamp = self._closest_approach_ts.get(key)
                attempt.closest_approach_cycle_number = self._closest_approach_cn.get(key)

            # -- Feature 2: Max Adverse Excursion on first leg --
            first_leg_bid = (
                snapshot.yes_bid_points
                if attempt.first_leg_side == Side.YES
                else snapshot.no_bid_points
            )
            if first_leg_bid is not None and first_leg_bid > 0:
                adverse = attempt.P1_points - first_leg_bid  # positive = loss
                adverse = max(0, adverse)  # MAE >= 0
                prev_mae = self._mae.get(key, 0)
                if adverse > prev_mae:
                    self._mae[key] = adverse
                    self._mae_ts[key] = cycle_time
                    self._mae_cn[key] = cycle_number
                attempt.max_adverse_excursion_points = self._mae.get(key, 0)
                attempt.mae_timestamp = self._mae_ts.get(key)
                attempt.mae_cycle_number = self._mae_cn.get(key)

        # Lifecycle records (only for pre-existing attempts with DB IDs)
        if self.enable_lifecycle:
            for attempt in self.active_attempts:
                if id(attempt) in pre_existing_ids:
                    opp_ask = (
                        snapshot.yes_ask_points
                        if attempt.opposite_side == Side.YES
                        else snapshot.no_ask_points
                    )
                    dist = (
                        (opp_ask - attempt.opposite_trigger_points)
                        if opp_ask is not None
                        else None
                    )
                    result.lifecycle_records.append(LifecycleRecord(
                        attempt_id=attempt.attempt_id,
                        cycle_number=cycle_number,
                        timestamp=cycle_time,
                        opposite_ask_points=opp_ask,
                        distance_to_trigger=dist,
                        closest_approach_so_far=self._closest_approach.get(
                            id(attempt)
                        ),
                    ))

        # Track concurrency peak
        self.max_concurrent = max(self.max_concurrent, len(self.active_attempts))
        result.active_count = len(self.active_attempts)
        result.pending_limit_count = len(self._pending_limits)

        return result

    # ------------------------------------------------------------------
    # Settlement
    # ------------------------------------------------------------------

    def process_settlement(
        self, settlement_time: datetime, time_remaining: float = 0.0,
        fail_reason: str = "settlement_reached",
    ) -> list[Attempt]:
        """Mark every remaining active attempt as failed and discard pending limits."""
        # Discard pending limits (they never became attempts)
        self._pending_limits.clear()

        failed: list[Attempt] = []
        for attempt in self.active_attempts:
            attempt.status = AttemptStatus.COMPLETED_FAILED
            attempt.fail_reason = fail_reason
            attempt.time_remaining_at_completion = time_remaining
            key = id(attempt)

            # Finalize closest approach
            if key in self._closest_approach:
                attempt.closest_approach_points = self._closest_approach[key]
                attempt.closest_approach_timestamp = self._closest_approach_ts.get(key)
                attempt.closest_approach_cycle_number = self._closest_approach_cn.get(key)

            # Finalize MAE
            if key in self._mae:
                attempt.max_adverse_excursion_points = self._mae[key]
                attempt.mae_timestamp = self._mae_ts.get(key)
                attempt.mae_cycle_number = self._mae_cn.get(key)

            # Clean up trackers
            self._closest_approach.pop(key, None)
            self._closest_approach_ts.pop(key, None)
            self._closest_approach_cn.pop(key, None)
            self._mae.pop(key, None)
            self._mae_ts.pop(key, None)
            self._mae_cn.pop(key, None)

            self.total_failed += 1
            failed.append(attempt)
            logger.info(
                "Settlement: failed attempt #%d — %s-first, active %.1fs, "
                "closest=%s, mae=%s",
                attempt.attempt_id,
                attempt.first_leg_side.value,
                (settlement_time - attempt.t1_timestamp).total_seconds(),
                attempt.closest_approach_points,
                attempt.max_adverse_excursion_points,
            )

        self.active_attempts = []
        return failed

    # ------------------------------------------------------------------
    # Feed gap helper
    # ------------------------------------------------------------------

    def mark_feed_gap(self) -> None:
        """Flag all active attempts as having experienced a feed gap."""
        for attempt in self.active_attempts:
            attempt.had_feed_gap = True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_attempt_from_pending(
        self,
        pending: PendingLimit,
        ref_yes: int,
        ref_no: int,
        cycle_number: int,
        cycle_time: datetime,
        time_remaining: float,
        snapshot: Snapshot,
        cycles_to_fill: int,
    ) -> Attempt:
        """Build a new Attempt from a filled PendingLimit.

        P1 = the pending limit price (maker fill price).
        Opposite trigger = min(from_reference, from_pair_constraint),
        calculated from the snapshot at fill time (DYNAMIC_PER_ATTEMPT).
        """
        self._attempt_counter += 1
        self.total_attempts += 1

        first_leg_side = pending.side
        P1 = pending.limit_price_points
        opposite_side = first_leg_side.opposite

        # Opposite reference: use the pair constraint directly.
        # opp_max (PairCap - P1) is the hard ceiling.  We also compute a
        # reference-based trigger for extra conservatism.
        opp_ref = ref_yes if opposite_side == Side.YES else ref_no

        opp_trigger_from_ref = round_to_tick(
            opp_ref - self.params.S0_points, self.tick
        )
        opp_trigger_from_ref = clamp_trigger(opp_trigger_from_ref, self.tick)

        # Pair constraint: OppositeMax = PairCap − P1
        opp_max = round_to_tick(self.params.pair_cap_points - P1, self.tick)

        # Edge-case guards (spec §11.4 / §11.5)
        if opp_max > 100:
            logger.error(
                "ERROR_IMPOSSIBLE_OPPOSITEMAX: %d (P1=%d, PairCap=%d)",
                opp_max, P1, self.params.pair_cap_points,
            )
        if opp_max < self.tick:
            logger.warning(
                "pair_constraint_impossible: opp_max=%d < tick=%d",
                opp_max, self.tick,
            )
            opp_max = self.tick

        # Final trigger = stricter of the two
        opp_trigger = min(opp_trigger_from_ref, opp_max)

        # --- Feature 3: time remaining bucket ---
        if time_remaining > 600:
            bucket = "600s+"
        elif time_remaining > 300:
            bucket = "300-600s"
        elif time_remaining > 120:
            bucket = "120-300s"
        else:
            bucket = "0-120s"

        # --- Feature 5: spread at entry ---
        yes_spread_entry = (
            (snapshot.yes_ask_points - snapshot.yes_bid_points)
            if snapshot.yes_ask_points is not None and snapshot.yes_bid_points is not None
            else None
        )
        no_spread_entry = (
            (snapshot.no_ask_points - snapshot.no_bid_points)
            if snapshot.no_ask_points is not None and snapshot.no_bid_points is not None
            else None
        )

        # --- Limit order tracking ---
        placement_buffer = pending.ask_at_placement_points - P1

        attempt = Attempt(
            attempt_id=self._attempt_counter,
            market_id=self.market_info.market_slug,
            parameter_set_id=self.params.parameter_set_id or 0,
            cycle_number=cycle_number,
            t1_timestamp=cycle_time,
            first_leg_side=first_leg_side,
            P1_points=P1,
            reference_yes_points=ref_yes,
            reference_no_points=ref_no,
            opposite_side=opposite_side,
            opposite_trigger_points=opp_trigger,
            opposite_max_points=opp_max,
            time_remaining_at_start=time_remaining,
            time_remaining_bucket=bucket,
            yes_spread_entry_points=yes_spread_entry,
            no_spread_entry_points=no_spread_entry,
            # Denormalized for easier analytics
            delta_points=self.params.delta_points,
            S0_points=self.params.S0_points,
            # Limit order tracking fields
            limit_placed_timestamp=pending.placed_timestamp,
            limit_placed_cycle=pending.placed_cycle,
            cycles_to_fill_first_leg=cycles_to_fill,
            ask_at_placement_points=pending.ask_at_placement_points,
            placement_buffer_points=placement_buffer,
        )

        logger.info(
            "New attempt #%d: %s-first @ %dpt (buffer=%d, cycles_to_fill=%d) "
            "→ hunting %s <= %dpt (max=%d, from_ref=%d)",
            attempt.attempt_id,
            first_leg_side.value,
            P1,
            placement_buffer,
            cycles_to_fill,
            opposite_side.value,
            opp_trigger,
            opp_max,
            opp_trigger_from_ref,
        )

        return attempt

    @staticmethod
    def _has_valid_orderbook(snapshot: Snapshot) -> bool:
        """True if both sides have meaningful bid and ask data."""
        fields = [
            snapshot.yes_bid_points,
            snapshot.yes_ask_points,
            snapshot.no_bid_points,
            snapshot.no_ask_points,
        ]
        if any(f is None or f <= 0 for f in fields):
            return False
        if snapshot.yes_bid_points >= snapshot.yes_ask_points:
            return False
        if snapshot.no_bid_points >= snapshot.no_ask_points:
            return False
        return True
