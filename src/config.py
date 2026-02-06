"""Configuration loading and validation from config.yaml."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import yaml

from .models import SamplingMode, TriggerRule, ReferencePriceSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ParameterSetConfig:
    name: str
    S0_points: int
    delta_points: int
    trigger_rule: str
    reference_price_source: str


@dataclass
class SamplingConfig:
    mode: SamplingMode
    cycle_interval_seconds: float
    cycles_per_market: int


@dataclass
class MarketsConfig:
    crypto_assets: list[str]
    market_type: str
    discovery_poll_interval_seconds: int
    pre_discovery_lead_seconds: int


@dataclass
class DataConfig:
    database_path: str
    enable_snapshots: bool
    enable_lifecycle_tracking: bool


@dataclass
class QualityConfig:
    feed_gap_threshold_seconds: float
    max_reference_sum_deviation: int
    enable_sanity_checks: bool
    max_anomalies_per_market: int


@dataclass
class LoggingConfig:
    level: str
    file: Optional[str]
    console_dashboard: bool


@dataclass
class WebSocketConfig:
    url: str
    heartbeat_interval_seconds: int
    reconnect_max_delay_seconds: int
    rest_fallback_after_disconnect_seconds: int


@dataclass
class AppConfig:
    """Top-level application configuration."""
    parameter_sets: list[ParameterSetConfig]
    sampling: SamplingConfig
    markets: MarketsConfig
    data: DataConfig
    quality: QualityConfig
    logging: LoggingConfig
    websocket: WebSocketConfig


# ---------------------------------------------------------------------------
# Loading & Validation
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> AppConfig:
    """Load and validate configuration from a YAML file.

    Raises:
        FileNotFoundError: If the config file does not exist.
        ValueError: If validation fails.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    # --- Parameter sets ---
    param_sets: list[ParameterSetConfig] = []
    for ps in raw.get("parameter_sets", []):
        param_sets.append(ParameterSetConfig(
            name=ps["name"],
            S0_points=int(ps["S0_points"]),
            delta_points=int(ps["delta_points"]),
            trigger_rule=ps.get("trigger_rule", "ASK_TOUCH"),
            reference_price_source=ps.get("reference_price_source", "MIDPOINT"),
        ))

    # --- Sampling ---
    samp = raw.get("sampling", {})
    sampling = SamplingConfig(
        mode=SamplingMode(samp.get("mode", "FIXED_INTERVAL")),
        cycle_interval_seconds=float(samp.get("cycle_interval_seconds", 10)),
        cycles_per_market=int(samp.get("cycles_per_market", 90)),
    )

    # --- Markets ---
    mkts = raw.get("markets", {})
    markets = MarketsConfig(
        crypto_assets=mkts.get("crypto_assets", ["btc"]),
        market_type=mkts.get("market_type", "15m"),
        discovery_poll_interval_seconds=int(mkts.get("discovery_poll_interval_seconds", 60)),
        pre_discovery_lead_seconds=int(mkts.get("pre_discovery_lead_seconds", 120)),
    )

    # --- Data ---
    d = raw.get("data", {})
    data = DataConfig(
        database_path=d.get("database_path", "data/measurements.db"),
        enable_snapshots=bool(d.get("enable_snapshots", False)),
        enable_lifecycle_tracking=bool(d.get("enable_lifecycle_tracking", False)),
    )

    # --- Quality ---
    q = raw.get("quality", {})
    quality = QualityConfig(
        feed_gap_threshold_seconds=float(q.get("feed_gap_threshold_seconds", 10)),
        max_reference_sum_deviation=int(q.get("max_reference_sum_deviation", 2)),
        enable_sanity_checks=bool(q.get("enable_sanity_checks", True)),
        max_anomalies_per_market=int(q.get("max_anomalies_per_market", 50)),
    )

    # --- Logging ---
    lg = raw.get("logging", {})
    logging_cfg = LoggingConfig(
        level=lg.get("level", "INFO"),
        file=lg.get("file"),
        console_dashboard=bool(lg.get("console_dashboard", True)),
    )

    # --- WebSocket ---
    ws = raw.get("websocket", {})
    websocket = WebSocketConfig(
        url=ws.get("url", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        heartbeat_interval_seconds=int(ws.get("heartbeat_interval_seconds", 30)),
        reconnect_max_delay_seconds=int(ws.get("reconnect_max_delay_seconds", 60)),
        rest_fallback_after_disconnect_seconds=int(
            ws.get("rest_fallback_after_disconnect_seconds", 60)
        ),
    )

    config = AppConfig(
        parameter_sets=param_sets,
        sampling=sampling,
        markets=markets,
        data=data,
        quality=quality,
        logging=logging_cfg,
        websocket=websocket,
    )

    _validate_config(config)
    return config


def _validate_config(config: AppConfig) -> None:
    """Validate configuration values per spec §14.1."""
    errors: list[str] = []

    if not config.parameter_sets:
        errors.append("At least one parameter set is required")

    for ps in config.parameter_sets:
        if ps.S0_points < 0 or ps.S0_points >= 50:
            errors.append(f"S0_points must be in [0, 50), got {ps.S0_points}")
        if ps.delta_points <= 0 or ps.delta_points >= 50:
            errors.append(f"delta_points must be in (0, 50), got {ps.delta_points}")
        if ps.trigger_rule not in ("ASK_TOUCH",):
            errors.append(f"Unknown trigger_rule: {ps.trigger_rule}")
        if ps.reference_price_source not in ("MIDPOINT", "LAST_TRADE"):
            errors.append(f"Unknown reference_price_source: {ps.reference_price_source}")

    if config.sampling.cycle_interval_seconds <= 0:
        errors.append("cycle_interval_seconds must be > 0")
    if config.sampling.cycles_per_market <= 0:
        errors.append("cycles_per_market must be > 0")

    if not config.markets.crypto_assets:
        errors.append("At least one crypto asset is required")

    if config.quality.feed_gap_threshold_seconds <= 0:
        errors.append("feed_gap_threshold_seconds must be > 0")

    if errors:
        for e in errors:
            logger.error("Config validation error: %s", e)
        raise ValueError(f"Config validation failed: {'; '.join(errors)}")

    logger.info(
        "Config loaded and validated — %d parameter set(s), assets=%s, sampling=%s/%s",
        len(config.parameter_sets),
        config.markets.crypto_assets,
        config.sampling.mode.value,
        f"{config.sampling.cycle_interval_seconds}s"
        if config.sampling.mode == SamplingMode.FIXED_INTERVAL
        else f"{config.sampling.cycles_per_market} cycles",
    )
