"""Core trigger evaluation engine.

Implements the heart of the measurement bot:
  • Reference price calculation (midpoint)
  • Trigger condition checking (ASK_TOUCH)
  • Attempt creation with pair-constraint enforcement
  • Active attempt monitoring and pairing
  • Closest-approach tracking (always on)
  • Per-cycle lifecycle records (optional, high-volume)
  • Simultaneous trigger handling with tie-breaking
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

    Holds the list of active attempts and running counters.  The evaluator
    is pure compute — no I/O, no async.  The ``MarketMonitor`` feeds it
    snapshots and persists the returned results.
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
        """Evaluate trigger conditions at a scheduled measurement cycle.

        Follows the spec §9.3 processing steps:
          1. Validate orderbook
          2. Calculate reference prices
          3-4. Check YES / NO triggers
          5. Handle simultaneous triggers
          6. Update all active attempts
          7. Track closest approach + lifecycle
          8. Clean up completed
        """
        result = CycleResult()

        # --- Step 1: validate orderbook data ---
        if not self._has_valid_orderbook(snapshot):
            result.skipped = True
            result.skip_reason = "orderbook_empty"
            logger.warning("Cycle %d: skipped — incomplete orderbook", cycle_number)
            result.active_count = len(self.active_attempts)
            return result

        # --- Step 2: reference prices (midpoints) ---
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

        # --- Step 3 & 4: spread-based triggers ---
        #
        # Trigger fires when the combined ask spread is tight enough:
        #   YES_ask + NO_ask <= 100 + S0
        #
        # S0 = max spread above $1.00 that still starts an attempt.
        #   S0=0 → only if combined asks <= 100 (rare, very tight)
        #   S0=1 → combined asks <= 101 (typical tight market)
        #   S0=3 → combined asks <= 103 (wider tolerance)
        #
        # delta (via PairCap) only affects the PAIRING constraint, not the
        # initial trigger.  Higher delta = harder to pair, but same trigger rate.
        #
        pair_cap = self.params.pair_cap_points

        yes_trigger = round_to_tick(
            100 + self.params.S0_points - snapshot.no_ask_points,
            self.tick,
        )
        yes_trigger = clamp_trigger(yes_trigger, self.tick)
        yes_triggered = snapshot.yes_ask_points <= yes_trigger

        no_trigger = round_to_tick(
            100 + self.params.S0_points - snapshot.yes_ask_points,
            self.tick,
        )
        no_trigger = clamp_trigger(no_trigger, self.tick)
        no_triggered = snapshot.no_ask_points <= no_trigger

        # Remember pre-existing attempts (they have valid DB IDs)
        pre_existing_ids = set(id(a) for a in self.active_attempts)

        # --- Step 5: create new attempts (handle simultaneous) ---
        new_attempts: list[Attempt] = []
        ref_yes_int = int(yes_ref)
        ref_no_int = int(no_ref)

        # Pre-filter: skip sides where P1 would exceed PairCap (impossible pair)
        if yes_triggered and yes_trigger >= pair_cap:
            yes_triggered = False
            logger.debug(
                "Cycle %d: YES trigger suppressed — trig=%d >= PairCap=%d",
                cycle_number, yes_trigger, pair_cap,
            )
        if no_triggered and no_trigger >= pair_cap:
            no_triggered = False
            logger.debug(
                "Cycle %d: NO trigger suppressed — trig=%d >= PairCap=%d",
                cycle_number, no_trigger, pair_cap,
            )

        if yes_triggered and no_triggered:
            # Tie-break: side with larger distance below trigger
            yes_dist = yes_trigger - snapshot.yes_ask_points
            no_dist = no_trigger - snapshot.no_ask_points

            if yes_dist >= no_dist:
                first, second = Side.YES, Side.NO
                first_trig, second_trig = yes_trigger, no_trigger
            else:
                first, second = Side.NO, Side.YES
                first_trig, second_trig = no_trigger, yes_trigger

            new_attempts.append(self._create_attempt(
                first, first_trig, ref_yes_int, ref_no_int,
                cycle_number, cycle_time, time_remaining, snapshot,
            ))
            new_attempts.append(self._create_attempt(
                second, second_trig, ref_yes_int, ref_no_int,
                cycle_number, cycle_time, time_remaining, snapshot,
            ))
            logger.info(
                "Cycle %d: SIMULTANEOUS trigger — YES ask=%d trig=%d, "
                "NO ask=%d trig=%d",
                cycle_number,
                snapshot.yes_ask_points, yes_trigger,
                snapshot.no_ask_points, no_trigger,
            )

        elif yes_triggered:
            new_attempts.append(self._create_attempt(
                Side.YES, yes_trigger, ref_yes_int, ref_no_int,
                cycle_number, cycle_time, time_remaining, snapshot,
            ))
            logger.info(
                "Cycle %d: YES trigger — ask=%d <= trig=%d "
                "(PairCap=%d, S0=%d, NO_ask=%d, combined=%d)",
                cycle_number, snapshot.yes_ask_points, yes_trigger,
                pair_cap, self.params.S0_points, snapshot.no_ask_points,
                snapshot.yes_ask_points + snapshot.no_ask_points,
            )

        elif no_triggered:
            new_attempts.append(self._create_attempt(
                Side.NO, no_trigger, ref_yes_int, ref_no_int,
                cycle_number, cycle_time, time_remaining, snapshot,
            ))
            logger.info(
                "Cycle %d: NO trigger — ask=%d <= trig=%d "
                "(PairCap=%d, S0=%d, YES_ask=%d, combined=%d)",
                cycle_number, snapshot.no_ask_points, no_trigger,
                pair_cap, self.params.S0_points, snapshot.yes_ask_points,
                snapshot.yes_ask_points + snapshot.no_ask_points,
            )

        # Add new attempts to the active list
        self.active_attempts.extend(new_attempts)
        result.new_attempts = new_attempts

        # --- Step 6: update ALL active attempts (check for pairing) ---
        paired: list[Attempt] = []
        still_active: list[Attempt] = []

        for attempt in self.active_attempts:
            opp_ask = (
                snapshot.yes_ask_points
                if attempt.opposite_side == Side.YES
                else snapshot.no_ask_points
            )

            if opp_ask is not None and opp_ask <= attempt.opposite_trigger_points:
                # *** PAIRED ***
                attempt.status = AttemptStatus.COMPLETED_PAIRED
                attempt.t2_timestamp = cycle_time
                attempt.t2_cycle_number = cycle_number
                attempt.time_to_pair_seconds = (
                    (cycle_time - attempt.t1_timestamp).total_seconds()
                )
                attempt.actual_opposite_price = opp_ask
                attempt.pair_cost_points = attempt.P1_points + opp_ask
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
            opp_ask = (
                snapshot.yes_ask_points
                if attempt.opposite_side == Side.YES
                else snapshot.no_ask_points
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

        return result

    # ------------------------------------------------------------------
    # Settlement
    # ------------------------------------------------------------------

    def process_settlement(
        self, settlement_time: datetime, time_remaining: float = 0.0,
        fail_reason: str = "settlement_reached",
    ) -> list[Attempt]:
        """Mark every remaining active attempt as failed."""
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

    def _create_attempt(
        self,
        first_leg_side: Side,
        trigger_level: int,
        ref_yes: int,
        ref_no: int,
        cycle_number: int,
        cycle_time: datetime,
        time_remaining: float,
        snapshot: Snapshot,
    ) -> Attempt:
        """Build a new Attempt record.

        P1 = the trigger level that was just touched (spec §6.4).
        Opposite trigger = min(from_reference, from_pair_constraint).
        """
        self._attempt_counter += 1
        self.total_attempts += 1

        P1 = trigger_level
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
        )

        logger.info(
            "New attempt #%d: %s-first @ %dpt → hunting %s <= %dpt "
            "(max=%d, from_ref=%d)",
            attempt.attempt_id,
            first_leg_side.value,
            P1,
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
