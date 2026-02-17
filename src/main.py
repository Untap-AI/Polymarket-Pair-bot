"""Entry point for the Polymarket Pair Measurement Bot.

Monitors all configured crypto assets in parallel, automatically rotating
to the next 15-minute market window on settlement.  Supports multiple
parameter sets evaluated concurrently and an optional Rich dashboard.

Run with:  python -m src
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from .asset_manager import AssetManager
from .config import load_config
from .database import Database
from .logging_config import setup_logging
from .models import ParameterSet, ReferencePriceSource, TriggerRule
from .rest_client import CLOBRestClient

logger = logging.getLogger(__name__)

STATUS_INTERVAL = 30  # seconds between status log lines (non-dashboard mode)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    """Discover and monitor 15-min crypto markets across all configured assets."""

    # --- Load config ---
    config = load_config()

    # --- Determine dashboard mode ---
    use_dashboard = config.logging.console_dashboard
    try:
        from .dashboard import Dashboard, create_event_log  # noqa: F401
    except ImportError:
        use_dashboard = False

    # --- Logging ---
    setup_logging(
        level=config.logging.level,
        log_file=config.logging.file,
        enable_console=not use_dashboard,  # suppress console when dashboard is active
    )

    logger.info("=" * 60)
    logger.info("  POLYMARKET PAIR MEASUREMENT BOT")
    logger.info("  Assets: %s", ", ".join(a.upper() for a in config.markets.crypto_assets))
    logger.info("=" * 60)

    # --- Database ---
    db = Database(
        database_url=config.data.database_url,
        database_url_session=config.data.database_url_session,
        db_path=config.data.database_path,
    )
    await db.initialize()

    # --- Parameter sets (support multiple) ---
    params_list: list[ParameterSet] = []
    for ps_cfg in config.parameter_sets:
        ps = ParameterSet(
            name=ps_cfg.name,
            S0_points=ps_cfg.S0_points,
            delta_points=ps_cfg.delta_points,
            trigger_rule=TriggerRule(ps_cfg.trigger_rule),
            reference_price_source=ReferencePriceSource(ps_cfg.reference_price_source),
            stop_loss_threshold_points=ps_cfg.stop_loss_threshold_points,
        )
        await db.insert_parameter_set(
            ps,
            sampling_mode=config.sampling.mode.value,
            cycle_interval=config.sampling.cycle_interval_seconds,
            cycles_per_market=config.sampling.cycles_per_market,
            feed_gap_threshold=config.quality.feed_gap_threshold_seconds,
        )
        params_list.append(ps)

    primary_ps = params_list[0]
    params_display = (
        f"Parameter set: {primary_ps.name} "
        f"(S0={primary_ps.S0_points}, d={primary_ps.delta_points})"
    )
    if len(params_list) > 1:
        params_display += f" + {len(params_list) - 1} more"

    logger.info("Parameters: %s", params_display)

    # --- REST client (shared, for health check) ---
    rest_client = CLOBRestClient()
    logger.info("Checking CLOB API connectivity…")
    if await rest_client.check_health():
        logger.info("CLOB API is reachable")
    else:
        logger.warning("CLOB API health check failed — continuing with WebSocket only")

    # --- Shutdown event + event log ---
    shutdown_event = asyncio.Event()
    session_start = datetime.now(timezone.utc)

    event_log = None
    if use_dashboard:
        from .dashboard import create_event_log
        event_log = create_event_log()

    # --- Asset managers ---
    managers: list[AssetManager] = []
    for asset in config.markets.crypto_assets:
        am = AssetManager(
            crypto_asset=asset,
            params_list=params_list,
            config=config,
            database=db,
            rest_client=rest_client,
            shutdown_event=shutdown_event,
            event_log=event_log,
        )
        managers.append(am)

    logger.info("Starting %d asset manager(s)…", len(managers))

    # --- Launch tasks ---
    asset_tasks = [
        asyncio.create_task(am.run(), name=f"am-{am.crypto_asset}")
        for am in managers
    ]

    # Dashboard or periodic status
    if use_dashboard:
        from .dashboard import Dashboard
        dashboard = Dashboard(
            managers=managers,
            event_log=event_log,
            session_start=session_start,
            params_display=params_display,
            shutdown_event=shutdown_event,
        )
        display_task = asyncio.create_task(dashboard.run(), name="dashboard")
    else:
        display_task = asyncio.create_task(
            _periodic_status(managers, shutdown_event), name="status"
        )

    all_tasks = asset_tasks + [display_task]

    try:
        await asyncio.gather(*all_tasks)
    except asyncio.CancelledError:
        logger.info("Shutdown signal received")
    finally:
        shutdown_event.set()

        for t in all_tasks:
            if not t.done():
                t.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

        # Session summary
        _print_session_summary(managers, session_start)

        # Cleanup
        await rest_client.close()
        await db.close()
        logger.info("Shutdown complete")


# ---------------------------------------------------------------------------
# Periodic status (fallback when dashboard is off)
# ---------------------------------------------------------------------------

async def _periodic_status(
    managers: list[AssetManager], shutdown_event: asyncio.Event
) -> None:
    """Log a status line for each asset every STATUS_INTERVAL seconds."""
    while not shutdown_event.is_set():
        try:
            await asyncio.wait_for(
                asyncio.shield(shutdown_event.wait()), timeout=STATUS_INTERVAL
            )
            break
        except asyncio.TimeoutError:
            pass

        lines = ["--- STATUS ---"]
        for m in managers:
            lines.append(f"  [STATUS] {m.status_line}")
        logger.info("\n".join(lines))


# ---------------------------------------------------------------------------
# Session summary
# ---------------------------------------------------------------------------

def _print_session_summary(
    managers: list[AssetManager], session_start: datetime
) -> None:
    elapsed = (datetime.now(timezone.utc) - session_start).total_seconds()
    hours, rem = divmod(int(elapsed), 3600)
    mins, secs = divmod(rem, 60)
    elapsed_str = f"{hours}h {mins}m {secs}s" if hours else f"{mins}m {secs}s"

    total_markets = sum(m.markets_monitored for m in managers)
    total_attempts = sum(m.total_attempts for m in managers)
    total_pairs = sum(m.total_pairs for m in managers)
    total_failed = sum(m.total_failed for m in managers)
    pair_rate = total_pairs / max(1, total_attempts)

    print()
    print("=" * 62)
    print("  SESSION SUMMARY")
    print("=" * 62)
    print(f"  Runtime:           {elapsed_str}")
    print(f"  Markets monitored: {total_markets}")

    for m in managers:
        tag = m.crypto_asset.upper()
        att = m.total_attempts
        pr = m.total_pairs
        pct = (pr / max(1, att)) * 100
        print(f"    {tag}: {m.markets_monitored} markets | "
              f"{att} attempts | {pr} pairs ({pct:.1f}%)")

    print("-" * 62)
    print(f"  Total attempts:    {total_attempts}")
    print(f"  Total pairs:       {total_pairs}")
    print(f"  Total failed:      {total_failed}")
    print(f"  Overall pair rate: {pair_rate:.1%}")
    print("=" * 62)
    print()
