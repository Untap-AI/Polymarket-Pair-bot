"""Rich console dashboard for live bot monitoring.

Provides a real-time auto-refreshing display showing per-asset status,
session statistics, and recent event history.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

if TYPE_CHECKING:
    from .asset_manager import AssetManager

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared event log
# ---------------------------------------------------------------------------

EventLog = deque  # deque of (datetime, str_asset, str_message) tuples


def create_event_log(maxlen: int = 50) -> EventLog:
    """Create a shared event log (thread-safe within asyncio event loop)."""
    return deque(maxlen=maxlen)


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class Dashboard:
    """Auto-refreshing rich console dashboard."""

    REFRESH_INTERVAL = 2.0  # seconds between display updates

    def __init__(
        self,
        managers: list[AssetManager],
        event_log: EventLog,
        session_start: datetime,
        params_display: str,
        shutdown_event: asyncio.Event,
    ):
        self.managers = managers
        self.event_log = event_log
        self.session_start = session_start
        self.params_display = params_display
        self.shutdown_event = shutdown_event
        self.console = Console()

    async def run(self) -> None:
        """Run the live dashboard until shutdown."""
        with Live(
            self._render(),
            console=self.console,
            refresh_per_second=0.5,
            screen=False,
        ) as live:
            while not self.shutdown_event.is_set():
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self.shutdown_event.wait()),
                        timeout=self.REFRESH_INTERVAL,
                    )
                    break
                except asyncio.TimeoutError:
                    pass
                try:
                    live.update(self._render())
                except Exception:
                    pass  # Don't crash the bot if rendering fails

    # ------------------------------------------------------------------
    # Rendering
    # ------------------------------------------------------------------

    def _render(self) -> Group:
        """Build the complete dashboard display."""
        now = datetime.now(timezone.utc)
        return Group(
            self._header_panel(now),
            self._asset_table(now),
            self._session_panel(now),
            self._events_panel(),
        )

    def _header_panel(self, now: datetime) -> Panel:
        elapsed = (now - self.session_start).total_seconds()
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        uptime = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

        text = Text()
        text.append("POLYMARKET PAIR MEASUREMENT BOT\n", style="bold white")
        text.append(
            f"Running since: {self.session_start.strftime('%Y-%m-%d %H:%M:%S UTC')}  "
            f"|  Uptime: {uptime}  |  {self.params_display}",
            style="dim",
        )
        return Panel(text, box=box.DOUBLE_EDGE, style="blue")

    def _asset_table(self, now: datetime) -> Table:
        table = Table(
            box=box.ROUNDED, expand=True, title="Live Markets",
            title_style="bold",
        )
        table.add_column("Asset", style="bold cyan", width=6)
        table.add_column("Market", width=24)
        table.add_column("Time Left", justify="right", width=10)
        table.add_column("Cycle", justify="right", width=9)
        table.add_column("Attempts", justify="right", width=9)
        table.add_column("Pairs", justify="right", width=6)
        table.add_column("%", justify="right", width=5)
        table.add_column("Active", justify="right", width=7)

        for am in self.managers:
            row = self._asset_row(am, now)
            table.add_row(*row)

        return table

    def _asset_row(self, am: AssetManager, now: datetime) -> tuple[str, ...]:
        tag = am.crypto_asset.upper()
        monitor = am._current_monitor

        if am._status == "monitoring" and monitor:
            mi = monitor.market_info
            ev = monitor.evaluator
            remaining = max(0, (mi.settlement_time - now).total_seconds())
            mins, secs = divmod(int(remaining), 60)
            total_att = ev.total_attempts
            total_pair = ev.total_pairs
            pct = f"{(total_pair / max(1, total_att)) * 100:.0f}%"
            active = str(len(ev.active_attempts))
            # Shorten slug: remove asset prefix for readability
            slug_short = mi.market_slug.replace(
                f"{am.crypto_asset}-updown-", ""
            )
            return (
                tag,
                slug_short,
                f"{mins}m {secs:02d}s",
                f"{monitor.cycles_run}/{monitor.total_planned_cycles}",
                str(total_att),
                str(total_pair),
                pct,
                active,
            )

        if am._status == "discovering":
            return (tag, "[yellow]discovering...[/]", "-", "-", "-", "-", "-", "-")

        return (tag, f"[dim]{am._status}[/]", "-", "-", "-", "-", "-", "-")

    def _session_panel(self, now: datetime) -> Panel:
        elapsed = (now - self.session_start).total_seconds()
        h, rem = divmod(int(elapsed), 3600)
        m, s = divmod(rem, 60)
        uptime = f"{h}h {m}m {s}s" if h else f"{m}m {s}s"

        total_markets = sum(am.markets_monitored for am in self.managers)
        total_att = sum(am.total_attempts for am in self.managers)
        total_pairs = sum(am.total_pairs for am in self.managers)
        pair_rate = total_pairs / max(1, total_att)

        # Count currently active attempts
        active_now = 0
        for am in self.managers:
            if am._current_monitor:
                active_now += len(am._current_monitor.evaluator.active_attempts)

        rotations = max(0, total_markets - len(self.managers))

        lines = [
            f"Uptime: {uptime}  |  Markets: {total_markets}  "
            f"|  Rotations: {rotations}",
            f"Attempts: {total_att}  |  Pairs: {total_pairs}  "
            f"|  Pair rate: {pair_rate:.1%}",
            f"Active attempts now: {active_now}",
        ]
        return Panel(
            "\n".join(lines),
            title="Session Totals",
            box=box.ROUNDED,
        )

    def _events_panel(self) -> Panel:
        events = list(self.event_log)[-10:]
        if not events:
            content = "  [dim]No events yet[/]"
        else:
            lines: list[str] = []
            for ts, asset, msg in reversed(events):
                time_str = ts.strftime("%H:%M:%S")
                if "PAIRED" in msg:
                    lines.append(f"  [green]{time_str}  {asset}  {msg}[/]")
                elif "started" in msg.lower():
                    lines.append(f"  {time_str}  {asset}  {msg}")
                elif "rotation" in msg.lower() or "settled" in msg.lower():
                    lines.append(f"  [yellow]{time_str}  {asset}  {msg}[/]")
                else:
                    lines.append(f"  {time_str}  {asset}  {msg}")
            content = "\n".join(lines)

        return Panel(
            content,
            title="Recent Events (last 10)",
            box=box.ROUNDED,
        )
