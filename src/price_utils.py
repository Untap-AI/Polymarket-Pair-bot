"""Decimal-based price/point conversion and tick rounding utilities.

All prices are internally represented as integer points (1 point = $0.01).
Decimal is used for all arithmetic to avoid floating-point errors.
"""

import math
from decimal import Decimal


def price_to_points(price_str: str) -> int:
    """Convert a price string (e.g. '0.45') to integer points (45).

    Uses Decimal for exact conversion.
    """
    return int(Decimal(price_str) * 100)


def points_to_price(points: int) -> Decimal:
    """Convert integer points (45) to Decimal price (Decimal('0.45'))."""
    return Decimal(points) / Decimal(100)


def round_to_tick(raw_points: int | float, tick_size_points: int) -> int:
    """Round a point value DOWN to the nearest tick increment (floor).

    Args:
        raw_points: The raw calculated point value (may be float from midpoint division).
        tick_size_points: The tick size in points (e.g. 1 for $0.01 tick).

    Returns:
        The floored point value aligned to the tick size.
    """
    if tick_size_points <= 0:
        raise ValueError(f"tick_size_points must be positive, got {tick_size_points}")
    return int(math.floor(raw_points / tick_size_points)) * tick_size_points


def clamp_trigger(trigger_points: int, tick_size_points: int) -> int:
    """Clamp a trigger price to the valid range [tick_size, 99].

    Args:
        trigger_points: The calculated trigger price in points.
        tick_size_points: Minimum tick size in points.

    Returns:
        The clamped trigger price.
    """
    lower = tick_size_points
    upper = 99
    return max(lower, min(trigger_points, upper))


def midpoint_points(bid_points: int, ask_points: int) -> float:
    """Calculate midpoint between bid and ask in points.

    Returns float because the midpoint may not be an integer
    (e.g. bid=45, ask=46 → midpoint=45.5).
    """
    return (bid_points + ask_points) / 2.0
