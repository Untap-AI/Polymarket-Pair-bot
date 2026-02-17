"""Configuration loading and validation.

Priority: environment variables > config.yaml > hardcoded defaults.

Environment variables (all optional — fall back to config.yaml then defaults):
    DATABASE_URL              PostgreSQL connection string (env-only, no YAML equivalent)
    DELTA_POINTS              Comma-separated deltas, e.g. "3,4,5,6,7,8,9,10"
    STOP_LOSS_THRESHOLD       Comma-separated stop loss thresholds in points, e.g. "1,2,3"
                              Creates cartesian product with DELTA_POINTS. Omit for no stop loss.
    S0_POINTS                 Spread offset (shared by all generated param sets)
    TRIGGER_RULE              "ASK_TOUCH"
    REFERENCE_PRICE_SOURCE    "MIDPOINT" or "LAST_TRADE"
    CRYPTO_ASSETS             Comma-separated, e.g. "btc,eth,sol,xrp"
    SAMPLING_MODE             "FIXED_INTERVAL" or "FIXED_COUNT"
    CYCLE_INTERVAL_SECONDS    e.g. "10"
    CYCLES_PER_MARKET         e.g. "90"
    LOG_LEVEL                 "DEBUG", "INFO", "WARNING", "ERROR"
    LOG_FILE                  Path or empty to disable
    CONSOLE_DASHBOARD         "true" or "false"
    ENABLE_SNAPSHOTS          "true" or "false"
    ENABLE_LIFECYCLE_TRACKING "true" or "false"
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml

from .models import SamplingMode, TriggerRule, ReferencePriceSource

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# .env file loader
# ---------------------------------------------------------------------------

def load_env_file() -> None:
    """Load .env file if it exists (simple key=value parser).
    
    Called automatically by load_config(), but can be called manually
    by scripts that need env vars before importing config.
    """
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        return
    
    try:
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                # Skip comments and empty lines
                if not line or line.startswith("#"):
                    continue
                # Parse KEY=value (handles quoted values)
                if "=" in line:
                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip()
                    # Remove quotes if present
                    if value.startswith('"') and value.endswith('"'):
                        value = value[1:-1]
                    elif value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    # Only set if not already in environment
                    if key and key not in os.environ:
                        os.environ[key] = value
    except Exception as e:
        logger.debug("Failed to load .env file: %s", e)


# ---------------------------------------------------------------------------
# Config dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ParameterSetConfig:
    name: str
    S0_points: int
    delta_points: int
    trigger_rule: str = "ASK_TOUCH"
    reference_price_source: str = "MIDPOINT"
    stop_loss_threshold_points: Optional[int] = None  # None = no stop loss


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
    database_url: Optional[str]
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
# Env-var helpers
# ---------------------------------------------------------------------------

def _env_bool(key: str, default: bool) -> bool:
    """Read a boolean from an environment variable."""
    val = os.environ.get(key)
    if val is None:
        return default
    return val.lower() in ("true", "1", "yes")


def _env(key: str, fallback):
    """Return env var *key* if set, otherwise *fallback*."""
    val = os.environ.get(key)
    return val if val is not None else fallback


# ---------------------------------------------------------------------------
# Parameter set loading
# ---------------------------------------------------------------------------

def _load_parameter_sets(raw: dict) -> list[ParameterSetConfig]:
    """Build parameter sets from env vars or YAML.

    Creates a cartesian product of S0 × delta × stop_loss values.
    Example: S0_POINTS=1,2  DELTA_POINTS=3,5  STOP_LOSS_THRESHOLD=1,2  → 8 param sets.
    S0_POINTS defaults to "1" (single value) for backward compatibility.
    If STOP_LOSS_THRESHOLD is not set, generates sets with no stop loss.
    """
    delta_env = os.environ.get("DELTA_POINTS")

    if delta_env:
        # Env-var driven: cartesian product of s0 × deltas × stop losses
        s0_env = os.environ.get("S0_POINTS", "1")
        s0_values = [int(s.strip()) for s in s0_env.split(",")]
        trigger_rule = os.environ.get("TRIGGER_RULE", "ASK_TOUCH")
        ref_source = os.environ.get("REFERENCE_PRICE_SOURCE", "MIDPOINT")

        deltas = [int(d.strip()) for d in delta_env.split(",")]

        sl_env = os.environ.get("STOP_LOSS_THRESHOLD")
        stop_losses: list[Optional[int]] = (
            [None if (v := int(s.strip())) == 0 else v for s in sl_env.split(",")]
            if sl_env
            else [None]
        )

        multi_s0 = len(s0_values) > 1
        param_sets: list[ParameterSetConfig] = []
        for s0 in s0_values:
            for d in deltas:
                for sl in stop_losses:
                    if multi_s0:
                        name = f"s0-{s0}-delta-{d}" if sl is None else f"s0-{s0}-delta-{d}-sl-{sl}"
                    else:
                        name = f"delta-{d}" if sl is None else f"delta-{d}-sl-{sl}"
                    param_sets.append(ParameterSetConfig(
                        name=name,
                        S0_points=s0,
                        delta_points=d,
                        trigger_rule=trigger_rule,
                        reference_price_source=ref_source,
                        stop_loss_threshold_points=sl,
                    ))
        return param_sets

    # Fall back to YAML
    param_sets: list[ParameterSetConfig] = []
    for ps in raw.get("parameter_sets", []):
        param_sets.append(ParameterSetConfig(
            name=ps["name"],
            S0_points=int(ps["S0_points"]),
            delta_points=int(ps["delta_points"]),
            trigger_rule=ps.get("trigger_rule", "ASK_TOUCH"),
            reference_price_source=ps.get("reference_price_source", "MIDPOINT"),
            stop_loss_threshold_points=(
                None if (sl := ps.get("stop_loss_threshold_points")) == 0 else sl
            ),
        ))

    # Ultimate fallback
    if not param_sets:
        param_sets.append(ParameterSetConfig(
            name="baseline",
            S0_points=1,
            delta_points=5,
        ))

    return param_sets


# ---------------------------------------------------------------------------
# Loading & Validation
# ---------------------------------------------------------------------------

def load_config(config_path: str = "config.yaml") -> AppConfig:
    """Load and validate configuration.

    Priority: environment variables > config.yaml > hardcoded defaults.
    The config file is optional — when running in Docker the env vars
    are sufficient on their own.

    Raises:
        ValueError: If validation fails.
    """
    # Load .env file first (if it exists)
    load_env_file()
    # --- Load YAML base (optional) ---
    raw: dict = {}
    path = Path(config_path)
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
    else:
        logger.info("Config file %s not found — using env vars / defaults", config_path)

    # --- Parameter sets ---
    param_sets = _load_parameter_sets(raw)

    # --- Sampling ---
    samp = raw.get("sampling", {})
    sampling = SamplingConfig(
        mode=SamplingMode(
            _env("SAMPLING_MODE", samp.get("mode", "FIXED_INTERVAL"))
        ),
        cycle_interval_seconds=float(
            _env("CYCLE_INTERVAL_SECONDS", samp.get("cycle_interval_seconds", 10))
        ),
        cycles_per_market=int(
            _env("CYCLES_PER_MARKET", samp.get("cycles_per_market", 90))
        ),
    )

    # --- Markets ---
    mkts = raw.get("markets", {})
    crypto_assets_env = os.environ.get("CRYPTO_ASSETS")
    crypto_assets = (
        [a.strip().lower() for a in crypto_assets_env.split(",")]
        if crypto_assets_env
        else mkts.get("crypto_assets", ["btc", "eth", "sol", "xrp"])
    )
    markets = MarketsConfig(
        crypto_assets=crypto_assets,
        market_type=_env("MARKET_TYPE", mkts.get("market_type", "15m")),
        discovery_poll_interval_seconds=int(
            _env("DISCOVERY_POLL_INTERVAL", mkts.get("discovery_poll_interval_seconds", 60))
        ),
        pre_discovery_lead_seconds=int(
            _env("PRE_DISCOVERY_LEAD", mkts.get("pre_discovery_lead_seconds", 120))
        ),
    )

    # --- Data ---
    d = raw.get("data", {})
    data = DataConfig(
        database_path=_env("DATABASE_PATH", d.get("database_path", "data/measurements.db")),
        database_url=os.environ.get("DATABASE_URL"),  # env-only (secret)
        enable_snapshots=_env_bool("ENABLE_SNAPSHOTS", d.get("enable_snapshots", False)),
        enable_lifecycle_tracking=_env_bool(
            "ENABLE_LIFECYCLE_TRACKING", d.get("enable_lifecycle_tracking", False)
        ),
    )

    # --- Quality ---
    q = raw.get("quality", {})
    quality = QualityConfig(
        feed_gap_threshold_seconds=float(
            _env("FEED_GAP_THRESHOLD", q.get("feed_gap_threshold_seconds", 10))
        ),
        max_reference_sum_deviation=int(
            _env("MAX_REF_SUM_DEVIATION", q.get("max_reference_sum_deviation", 2))
        ),
        enable_sanity_checks=_env_bool(
            "ENABLE_SANITY_CHECKS", q.get("enable_sanity_checks", True)
        ),
        max_anomalies_per_market=int(
            _env("MAX_ANOMALIES", q.get("max_anomalies_per_market", 50))
        ),
    )

    # --- Logging ---
    lg = raw.get("logging", {})
    logging_cfg = LoggingConfig(
        level=_env("LOG_LEVEL", lg.get("level", "INFO")),
        file=_env("LOG_FILE", lg.get("file", "logs/bot.log")),
        console_dashboard=_env_bool(
            "CONSOLE_DASHBOARD", lg.get("console_dashboard", True)
        ),
    )

    # --- WebSocket ---
    ws = raw.get("websocket", {})
    websocket = WebSocketConfig(
        url=_env(
            "WS_URL",
            ws.get("url", "wss://ws-subscriptions-clob.polymarket.com/ws/market"),
        ),
        heartbeat_interval_seconds=int(
            _env("WS_HEARTBEAT", ws.get("heartbeat_interval_seconds", 30))
        ),
        reconnect_max_delay_seconds=int(
            _env("WS_RECONNECT_MAX_DELAY", ws.get("reconnect_max_delay_seconds", 60))
        ),
        rest_fallback_after_disconnect_seconds=int(
            _env("REST_FALLBACK_DELAY", ws.get("rest_fallback_after_disconnect_seconds", 60))
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
    """Validate configuration values per spec S14.1."""
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
        if ps.stop_loss_threshold_points is not None:
            if ps.stop_loss_threshold_points <= 0 or ps.stop_loss_threshold_points >= 50:
                errors.append(
                    f"stop_loss_threshold_points must be in (0, 50), "
                    f"got {ps.stop_loss_threshold_points}"
                )

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

    # --- Summary log ---
    ps_names = ", ".join(ps.name for ps in config.parameter_sets)
    db_backend = "PostgreSQL" if config.data.database_url else "SQLite"
    logger.info(
        "Config loaded — %d param set(s) [%s], assets=%s, sampling=%s/%s, db=%s",
        len(config.parameter_sets),
        ps_names,
        config.markets.crypto_assets,
        config.sampling.mode.value,
        f"{config.sampling.cycle_interval_seconds}s"
        if config.sampling.mode == SamplingMode.FIXED_INTERVAL
        else f"{config.sampling.cycles_per_market} cycles",
        db_backend,
    )
