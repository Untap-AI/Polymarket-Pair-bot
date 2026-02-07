"""Data models for the Polymarket Pair Measurement Bot.

All price fields use integer points (1 point = $0.01).
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class Side(str, Enum):
    YES = "YES"
    NO = "NO"

    @property
    def opposite(self) -> "Side":
        return Side.NO if self == Side.YES else Side.YES


class AttemptStatus(str, Enum):
    ACTIVE = "active"
    COMPLETED_PAIRED = "completed_paired"
    COMPLETED_FAILED = "completed_failed"


class SamplingMode(str, Enum):
    FIXED_INTERVAL = "FIXED_INTERVAL"
    FIXED_COUNT = "FIXED_COUNT"


class TriggerRule(str, Enum):
    ASK_TOUCH = "ASK_TOUCH"


class ReferencePriceSource(str, Enum):
    MIDPOINT = "MIDPOINT"
    LAST_TRADE = "LAST_TRADE"


# ---------------------------------------------------------------------------
# Core dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ParameterSet:
    """A set of measurement parameters."""
    name: str
    S0_points: int
    delta_points: int
    trigger_rule: TriggerRule
    reference_price_source: ReferencePriceSource
    parameter_set_id: Optional[int] = None

    @property
    def pair_cap_points(self) -> int:
        """PairCap = 100 − δ."""
        return 100 - self.delta_points


@dataclass
class TokenOrderbook:
    """Orderbook state for a single token (YES or NO side)."""
    asset_id: str
    best_bid: Optional[int] = None          # points
    best_ask: Optional[int] = None          # points
    best_bid_size: Optional[str] = None
    best_ask_size: Optional[str] = None
    last_trade_price: Optional[int] = None  # points
    last_update: Optional[datetime] = None


@dataclass
class MarketInfo:
    """Metadata for a discovered Polymarket 15-min crypto market."""
    market_slug: str
    condition_id: str
    crypto_asset: str
    yes_token_id: str       # 70+ digit string — always TEXT
    no_token_id: str        # 70+ digit string — always TEXT
    settlement_time: datetime
    tick_size_points: int
    active: bool = True
    accepting_orders: bool = True


@dataclass
class Snapshot:
    """Orderbook snapshot captured at a measurement cycle."""
    market_id: str
    cycle_number: int
    timestamp: datetime
    yes_bid_points: Optional[int]
    yes_ask_points: Optional[int]
    no_bid_points: Optional[int]
    no_ask_points: Optional[int]
    yes_last_trade_points: Optional[int] = None
    no_last_trade_points: Optional[int] = None
    time_remaining_seconds: float = 0.0
    active_attempts_count: int = 0
    anomaly_flag: bool = False


@dataclass
class Attempt:
    """A single measurement attempt tracking one potential hedged pair."""
    attempt_id: int
    market_id: str
    parameter_set_id: int
    cycle_number: int
    t1_timestamp: datetime
    first_leg_side: Side
    P1_points: int
    reference_yes_points: int
    reference_no_points: int
    opposite_side: Side
    opposite_trigger_points: int
    opposite_max_points: int
    status: AttemptStatus = AttemptStatus.ACTIVE
    t2_timestamp: Optional[datetime] = None
    t2_cycle_number: Optional[int] = None
    time_to_pair_seconds: Optional[float] = None
    time_remaining_at_start: float = 0.0
    time_remaining_at_completion: Optional[float] = None
    actual_opposite_price: Optional[int] = None
    pair_cost_points: Optional[int] = None
    pair_profit_points: Optional[int] = None
    fail_reason: Optional[str] = None
    had_feed_gap: bool = False

    # --- Feature 1: Closest approach to opposite trigger ---
    closest_approach_points: Optional[int] = None     # min(opp_ask - opp_trigger) over lifetime; 0 or negative = crossed
    closest_approach_timestamp: Optional[datetime] = None
    closest_approach_cycle_number: Optional[int] = None

    # --- Feature 2: Max Adverse Excursion (MAE) on first leg ---
    max_adverse_excursion_points: Optional[int] = None  # max(P1 - first_leg_bid); always >= 0
    mae_timestamp: Optional[datetime] = None
    mae_cycle_number: Optional[int] = None

    # --- Feature 3: Time remaining bucket at entry ---
    time_remaining_bucket: Optional[str] = None  # "0-120s", "120-300s", "300-600s", "600s+"

    # --- Feature 5: Spread at entry and completion ---
    yes_spread_entry_points: Optional[int] = None  # yes_ask - yes_bid at t1
    no_spread_entry_points: Optional[int] = None   # no_ask - no_bid at t1
    yes_spread_exit_points: Optional[int] = None   # yes_ask - yes_bid at t2 (paired only)
    no_spread_exit_points: Optional[int] = None    # no_ask - no_bid at t2 (paired only)

    # --- Denormalized from ParameterSets for easier analytics ---
    delta_points: Optional[int] = None
    S0_points: Optional[int] = None


@dataclass
class LifecycleRecord:
    """Per-cycle tracking row for an active attempt.

    Written to the ``AttemptLifecycle`` table when
    ``enable_lifecycle_tracking`` is on.  High-volume — disabled by default.
    """
    attempt_id: int
    cycle_number: int
    timestamp: datetime
    opposite_ask_points: Optional[int]
    distance_to_trigger: Optional[int]
    closest_approach_so_far: Optional[int]


@dataclass
class MarketState:
    """Combined runtime state for an active market being monitored."""
    market_info: MarketInfo
    yes_orderbook: TokenOrderbook
    no_orderbook: TokenOrderbook
    is_active: bool = True
    total_attempts: int = 0
    total_pairs: int = 0
    total_failed: int = 0
    total_cycles_run: int = 0
    anomaly_count: int = 0
    max_concurrent_attempts: int = 0
    last_ws_message_time: Optional[datetime] = None

    def get_orderbook(self, side: Side) -> TokenOrderbook:
        """Return the orderbook for the given side."""
        return self.yes_orderbook if side == Side.YES else self.no_orderbook
