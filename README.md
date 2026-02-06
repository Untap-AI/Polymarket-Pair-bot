# Polymarket Pair Measurement Bot

A passive measurement tool that observes Polymarket's 15-minute crypto binary prediction markets (BTC, ETH, SOL, XRP up/down) and measures hedged pair trading opportunities without placing actual orders.

## Features

- **Multi-asset monitoring**: Simultaneously tracks BTC, ETH, SOL, and XRP markets
- **Continuous market rotation**: Automatically transitions between 15-minute market windows
- **Real-time WebSocket data**: Live orderbook updates from Polymarket CLOB
- **Comprehensive measurement**: Tracks attempts, pairs, time-to-pair, profit margins, MAE, spreads, and more
- **Rich analysis**: Built-in reporting with detailed metrics and CSV export
- **Parameter sweeps**: Test multiple parameter sets simultaneously

## Quick Start

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure**:
   Edit `config.yaml` to set your parameters (S0, delta, assets, etc.)

3. **Run**:
   ```bash
   python -m src
   ```

4. **Analyze results**:
   ```bash
   python scripts/analyze_results.py
   python scripts/export_data.py attempts --output data/attempts.csv
   ```

## Configuration

Key parameters in `config.yaml`:
- `S0_points`: Spread tolerance for triggering attempts (0-50)
- `delta_points`: Required profit margin (affects PairCap = 100 - delta)
- `crypto_assets`: List of assets to monitor (btc, eth, sol, xrp)
- `cycle_interval_seconds`: How often to run measurement cycles (default: 10s)

## Measurement Features

The bot tracks:
- **Closest approach**: Minimum distance to opposite trigger (with timestamps)
- **Max Adverse Excursion (MAE)**: Worst mark-to-market loss on first leg
- **Time remaining buckets**: Segments attempts by time-to-settlement at entry
- **Spread analysis**: Entry and exit spreads for both sides
- **Pair rate by market minute**: 5 × 3-minute buckets within each 15-min window

## Project Structure

```
├── src/                    # Main application code
│   ├── main.py            # Entry point
│   ├── market_monitor.py  # Per-market orchestration
│   ├── trigger_evaluator.py  # Core measurement logic
│   ├── websocket_client.py   # Real-time data feed
│   └── ...
├── scripts/               # Analysis and export tools
│   ├── analyze_results.py
│   └── export_data.py
├── config.yaml           # Configuration
└── PROJECT_SPEC.md       # Full specification

```

## Data Storage

All data is stored in SQLite (`data/measurements.db`):
- **Attempts**: Every trigger event with full lifecycle tracking
- **Markets**: Summary stats per 15-minute market window
- **Snapshots**: Optional per-cycle orderbook snapshots
- **AttemptLifecycle**: Optional detailed per-cycle tracking

## License

See LICENSE file (if applicable)
