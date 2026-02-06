# POLYMARKET PAIR MEASUREMENT BOT — COMPLETE PROJECT SPECIFICATION

## PROJECT CONTEXT

This specification was developed collaboratively between the project owner (James) and Claude (claude.ai). James will be feeding you instructions in phases. **Build Phase 1 only right now**, but read and understand the FULL document so your architectural decisions support the complete vision.

When you have questions or need clarification, James can consult with Claude on claude.ai to work through the answer. Treat this as a three-way collaboration: you build, James directs, Claude advises on the plan.

---

## TABLE OF CONTENTS

1. [Objective](#1-objective)
2. [Architecture Overview](#2-architecture-overview)
3. [Polymarket API Integration](#3-polymarket-api-integration)
4. [Definitions and Terminology](#4-definitions-and-terminology)
5. [Parameter Definitions](#5-parameter-definitions)
6. [Normalization and Calculation Rules](#6-normalization-and-calculation-rules)
7. [Sampling and Measurement Cycles](#7-sampling-and-measurement-cycles)
8. [Trigger Evaluation](#8-trigger-evaluation)
9. [State Machine Specification](#9-state-machine-specification)
10. [Data Model Specification](#10-data-model-specification)
11. [Edge Case Resolutions](#11-edge-case-resolutions)
12. [Market Discovery and Rotation](#12-market-discovery-and-rotation)
13. [Output Metrics and Analysis](#13-output-metrics-and-analysis)
14. [Validation and Sanity Checks](#14-validation-and-sanity-checks)
15. [Known Limitations](#15-known-limitations)
16. [Repository Structure](#16-repository-structure)
17. [Configuration](#17-configuration)
18. [Build Phases](#18-build-phases)

---

## 1. OBJECTIVE

Measure the feasibility of creating hedged pairs in Polymarket's 15-minute binary crypto prediction markets (BTC, ETH, SOL, XRP up/down) by observing price movements **without placing actual orders**.

This bot is a **passive measurement tool**. It connects to live markets, watches orderbook data, and simulates whether hedged pair opportunities would have been capturable. It stores all results in a local database for analysis.

For each market and parameter configuration, measure:

1. **Attempts per market**: How many times a first trigger condition is met
2. **Pairs per market**: How many attempts successfully complete with both sides filling before settlement
3. **Pair rate**: Percentage of attempts that successfully pair (pairs ÷ attempts)
4. **Time-to-pair distribution**: How long it takes for the second side to fill after the first
5. **Settlement failures**: How many attempts are still open when market settles

### Key Design Philosophy

- Measurement cycles run on a **fixed time schedule** (configurable interval or count), not triggered by price events
- At each scheduled cycle, the bot takes a snapshot and evaluates trigger conditions
- Attempts are **overlapping and continuous** — new attempts start whenever trigger conditions are met at a scheduled cycle, regardless of other active attempts
- Each attempt is **independent** — tracks only whether its specific pair completes before settlement
- No inventory constraints — this is pure measurement of mean reversion frequency
- Attempts run until either **pairing or settlement**, whichever comes first
- The bot monitors **all active 15-minute crypto markets** simultaneously and automatically transitions to the next market window when one settles

---

## 2. ARCHITECTURE OVERVIEW

### Tech Stack

- **Python 3.11+** with asyncio
- **py-clob-client** — official Polymarket CLOB SDK (REST calls)
- **websockets** — WebSocket connection for real-time data
- **aiosqlite** — async SQLite access
- **No authentication required** — all data is read-only public market data

### Data Flow

```
┌─────────────────────────────────────────────────────────┐
│                   MARKET DISCOVERER                      │
│  Polls Gamma API for active 15-min crypto markets.       │
│  Pre-discovers next market before current one settles.   │
│  Creates/retires MarketMonitors as markets open/close.   │
└──────────────────────┬──────────────────────────────────┘
                       │ spawns per market
                       ▼
┌─────────────────────────────────────────────────────────┐
│              MARKET MONITOR (one per active market)       │
│  1. Subscribes to WebSocket for YES + NO token IDs       │
│  2. Maintains local orderbook state from WS events       │
│  3. Runs SCHEDULED CYCLES on fixed interval/count        │
│  4. At each cycle → takes snapshot → runs evaluator      │
│  5. At settlement → finalizes all attempts               │
│  6. Hands off to next market window seamlessly           │
└──────────────────────┬──────────────────────────────────┘
                       │ calls at each cycle
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  TRIGGER EVALUATOR                        │
│  Per scheduled cycle:                                    │
│  1. Capture current orderbook snapshot                   │
│  2. Calculate reference prices (midpoint)                │
│  3. Check YES trigger (ask <= trigger_level)             │
│  4. Check NO trigger (ask <= trigger_level)              │
│  5. Start new attempts if triggered                      │
│  6. Update ALL active attempts (check for pairs)         │
│  7. Handle simultaneous triggers                         │
└──────────────────────┬──────────────────────────────────┘
                       │ writes
                       ▼
┌─────────────────────────────────────────────────────────┐
│                    SQLite DATABASE                        │
│  Tables: ParameterSets, Markets, Attempts,               │
│          Snapshots (optional), AttemptLifecycle (optional)│
└─────────────────────────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────┐
│                  CONSOLE DASHBOARD                        │
│  Real-time summary: active markets, attempts,            │
│  pair rate, active attempt count per market               │
└─────────────────────────────────────────────────────────┘
```

### Concurrency Model

- **asyncio single process**, multiple coroutines
- One long-running coroutine for market discovery (polls every 60s)
- One coroutine per active market (manages WebSocket + scheduled cycles)
- Shared SQLite writer with async write queue to prevent contention

---

## 3. POLYMARKET API INTEGRATION

### 3.1 APIs Required

| API | Base URL | Purpose | Auth? |
|-----|----------|---------|-------|
| **Gamma API** | `https://gamma-api.polymarket.com` | Market discovery — find active 15-min crypto markets, get token_ids, settlement times | No |
| **CLOB REST** | `https://clob.polymarket.com` | Orderbook snapshots, price/midpoint/spread, tick_size, server time | No |
| **CLOB WebSocket** | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Real-time orderbook updates | No |

### 3.2 Market Discovery (Gamma API)

**Endpoint**: `GET https://gamma-api.polymarket.com/markets`

15-minute crypto markets follow this pattern:
- Slug format: `{crypto}-updown-15m-{unix_timestamp}` (e.g., `btc-updown-15m-1768502700`)
- The timestamp in the slug is the settlement time
- Each market has TWO tokens: "Up" (YES equivalent) and "Down" (NO equivalent)
- Markets are `neg_risk: true` (binary complementary tokens)

**Discovery approach**:
- Poll Gamma API every ~60 seconds for active markets
- Filter by: `active=true`, slug contains `updown-15m`
- For each target crypto (btc, eth, sol, xrp), find the currently active market

**Key fields from market response**:
```json
{
  "condition_id": "0x...",
  "market_slug": "btc-updown-15m-1768502700",
  "tokens": [
    { "token_id": "12345...", "outcome": "Up", "price": 0.52 },
    { "token_id": "67890...", "outcome": "Down", "price": 0.48 }
  ],
  "minimum_tick_size": 0.01,
  "minimum_order_size": 5,
  "end_date_iso": "2026-02-05T12:15:00Z",
  "neg_risk": true,
  "active": true,
  "accepting_orders": true
}
```

**IMPORTANT**: Token IDs are 70+ digit number strings (e.g., `66165572830542895638033723077931657501549172243920223554030255390751841276554`). Always store as strings, never as integers.

**Do NOT guess slugs programmatically** — this is a known pain point (GitHub issue #244 on py-clob-client). Always query the Gamma API with filters.

### 3.3 Real-Time Data (WebSocket — Primary)

**Connection**: `wss://ws-subscriptions-clob.polymarket.com/ws/market`

**Subscribe on connect**:
```json
{
  "assets_ids": ["<up_token_id>", "<down_token_id>"],
  "type": "market"
}
```

**Add/remove subscriptions without reconnecting**:
```json
{"assets_ids": ["<new_token_id>"], "operation": "subscribe"}
{"assets_ids": ["<old_token_id>"], "operation": "unsubscribe"}
```

**Event types received**:

1. **`book`** — Full orderbook snapshot
```json
{
  "event_type": "book",
  "asset_id": "12345...",
  "market": "0x...",
  "bids": [{"price": "0.52", "size": "100.5"}, ...],
  "asks": [{"price": "0.53", "size": "80.0"}, ...],
  "timestamp": "1234567890000",
  "hash": "0x..."
}
```
- `bids[0]` = best bid (highest), `asks[0]` = best ask (lowest)
- Prices are **strings** — parse with `Decimal`

2. **`price_change`** — Best bid/ask update (lighter weight)
```json
{
  "event_type": "price_change",
  "asset_id": "12345...",
  "price_changes": [{"best_bid": "0.52", "best_ask": "0.53"}]
}
```

3. **`last_trade_price`** — Trade executed
```json
{
  "event_type": "last_trade_price",
  "asset_id": "12345...",
  "price": "0.52",
  "size": "100",
  "side": "BUY",
  "timestamp": 1234567890000
}
```

4. **`tick_size_change`** — Tick size changed (rare, but handle it)

**WebSocket management requirements**:
- Ping/pong heartbeat every ~30 seconds
- Auto-reconnect with exponential backoff on disconnect
- Resubscribe to all active asset_ids on reconnect
- Track last message timestamp for feed gap detection

### 3.4 REST API (Fallback + Validation)

If WebSocket is unreliable, fall back to polling these endpoints:

| Endpoint | Returns |
|----------|---------|
| `GET /book?token_id={id}` | Full orderbook (bids + asks) |
| `POST /books` with body `[{"token_id": "id1"}, {"token_id": "id2"}]` | Multiple orderbooks (batch) |
| `GET /price?token_id={id}&side=BUY` | Best price for buy side |
| `GET /midpoint?token_id={id}` | Midpoint price |
| `GET /spread?token_id={id}` | Spread |
| `GET /tick-size?token_id={id}` | Tick size |
| `GET /time` | Server timestamp (for clock sync) |

**Known issue**: The `/book` REST endpoint has been reported to return stale data (showing 0.01/0.99 on active markets). The `/price` and `/midpoint` endpoints are more reliable. **Prefer WebSocket data; use REST for validation and fallback only.**

**Orderbook REST response format**:
```json
{
  "market": "0x...",
  "asset_id": "12345...",
  "timestamp": "2023-10-01T12:00:00Z",
  "bids": [{"price": "0.52", "size": "100.5"}],
  "asks": [{"price": "0.53", "size": "80.0"}],
  "min_order_size": "5",
  "tick_size": "0.01",
  "neg_risk": true,
  "hash": "0x..."
}
```

### 3.5 Rate Limits

- REST: ~100 requests/minute for unauthenticated
- WebSocket: No explicit receive rate limit; subscription change requests are limited
- Use batch endpoints (`POST /books`) where possible
- Cache market discovery results (don't re-query Gamma on every cycle)

### 3.6 Mapping Spec Concepts to Polymarket

| This Spec Says | Polymarket Reality |
|---|---|
| "YES side" | The "Up" token (first token in market) |
| "NO side" | The "Down" token (second token in market) |
| "Best ask for YES" | `asks[0].price` from Up token orderbook |
| "Best bid for YES" | `bids[0].price` from Up token orderbook |
| "Market settles" | `end_date_iso` reached; market `active` → `false` |
| "Reference price = midpoint" | `(best_bid + best_ask) / 2` per side |
| "tick_size" | `minimum_tick_size` from market metadata (usually 0.01) |
| "Price of 0.45 = 45 points" | Convert: `int(Decimal(price_str) * 100)` |
| "100 points = $1.00" | Guaranteed payout on resolution |

---

## 4. DEFINITIONS AND TERMINOLOGY

**Pair**: A position where you hold equal quantities of both YES and NO shares such that the combined cost is less than the guaranteed payout of $1.00 per share. Example: 100 YES shares at 0.45 plus 100 NO shares at 0.48 equals 0.93 cost, guaranteeing 0.07 profit per share at resolution.

**Attempt**: A measurement period that begins when price touches a trigger level on one side during a scheduled measurement cycle. The attempt remains open and actively monitored on subsequent cycles for the opposite side to trigger. The attempt ends when either: (a) the opposite side triggers (success — pair formed), or (b) market settles (failure — pair never formed).

**Trigger**: The price level at which we simulate placing a buy order. When the best ask price is at or below this level during a scheduled cycle, we consider our theoretical limit order as filled and start a new attempt.

**Reference Price**: The baseline price from which trigger levels are calculated. **Dynamically recalculated** at the moment each attempt starts, based on current market conditions at that exact cycle's snapshot.

**Pair Capacity (PairCap)**: The maximum combined cost for both sides that still allows profitable pairing. Calculated as 100 points minus the desired profit margin delta.

**Measurement Cycle**: A scheduled point in time when the bot takes a market snapshot and evaluates all trigger conditions. Cycles run on a fixed schedule configured as either a time interval (e.g., every 10 seconds) or a target count per market window (e.g., 30 cycles per 15-min market). The bot auto-adjusts if it connects mid-market.

**Overlapping Attempts**: Multiple attempts can be active simultaneously. If YES triggers at cycle #3 and again at cycle #7 before the first attempt completes, both attempts continue monitoring independently.

---

## 5. PARAMETER DEFINITIONS

### 5.1 Fixed Parameters (Set Before Run)

**S0 (Initial Spread Offset)**: The distance in points below the reference price at which the trigger is set. Measured in hundredths of a dollar (points). Example: S0 of 5 means triggers are placed 0.05 below reference price.

**δ (Delta — Profit Margin)**: The minimum profit margin in points required for a successful pair. This determines the maximum combined cost. Example: δ of 3 means you require at least $0.03 profit per share, so maximum pair cost is $0.97.

**PairCap (Pair Capacity)**: Calculated as 100 minus delta. The maximum combined point cost for both sides.

**tick_size (Tick Size)**: Read dynamically from each market's metadata via the API. The minimum price increment allowed by the market, measured in points. All calculated trigger prices must be rounded to valid tick increments.

### 5.2 Rule Selections

**trigger_rule**: Defines what price movement constitutes a "fill."
- **ASK_TOUCH**: Trigger occurs when the best ask price is at or below the trigger level. This means a limit buy order at the trigger price would have been filled. This is the recommended and only option for v1.

**reference_rule**: Defines how reference prices are established.
- **DYNAMIC_PER_ATTEMPT**: Reference prices are calculated fresh at the exact cycle when each new attempt starts. This is the only option.

**reference_price_source**: How to calculate the reference price from market data.
- **MIDPOINT**: Use the average of best bid and best ask. More stable and less susceptible to outlier quotes. Recommended.
- **LAST_TRADE**: Use the most recent trade price. Can be stale.

**tie_break_rule**: What to do when both YES and NO trigger simultaneously in the same cycle.
- Cascade of three rules:
  1. Use the side with smaller distance between current price and trigger price (touched "harder")
  2. If still tied, consistently prefer YES side as fallback
  3. BOTH attempts are always created — tie-break only determines ordering

**sampling_rule**: How measurement cycles are scheduled.
- **FIXED_INTERVAL**: Run a cycle every X seconds (e.g., every 10 seconds). Number of cycles depends on time remaining.
- **FIXED_COUNT**: Run exactly N cycles per market window. Interval auto-calculated from time remaining.

Both options are available in config. The bot auto-adjusts if it connects to a market mid-window (e.g., 7 minutes remaining instead of 15).

---

## 6. NORMALIZATION AND CALCULATION RULES

### 6.1 Point Conversion

All prices internally use integer points (hundredths of a dollar) to avoid floating point errors.

- One point = $0.01
- Price of 0.45 = 45 points
- Price of 0.50 = 50 points

**Conversion from API strings**:
```python
from decimal import Decimal, ROUND_DOWN

def price_to_points(price_str: str) -> int:
    return int(Decimal(price_str) * 100)

def points_to_price(points: int) -> Decimal:
    return Decimal(points) / 100
```

**CRITICAL**: Use `Decimal` for all arithmetic. Never use float for price calculations.

### 6.2 Tick Rounding

Every calculated trigger price must align with the market's tick size. Rounding always uses floor (round down) to be conservative.

**RoundToTick function**:
- Divide the raw point value by tick size (in points)
- Take the floor (round down)
- Multiply back by tick size

Example with tick_size of 1 point:
- Raw trigger: 45.7 points → rounds to 45 points
- Raw trigger: 46.0 points → stays 46 points

### 6.3 Reference Price Calculation (Per Attempt Start)

When a new attempt is about to start (trigger condition met at a scheduled cycle):

**Step 1** — Use the market snapshot captured at this cycle's scheduled time.

**Step 2** — Calculate midpoints:
- ReferenceYes_points = (YES_best_bid_points + YES_best_ask_points) / 2
- ReferenceNo_points = (NO_best_bid_points + NO_best_ask_points) / 2

**Step 3** — Sanity check: Sum should be close to 100 points (within 2 points tolerance). If deviation exceeds 2 points, log anomaly warning but continue.

**Step 4** — Use immediately for this attempt only. The next attempt calculates fresh references.

### 6.4 Trigger Price Calculation (Per Attempt)

When an attempt starts, calculate where the opposite side's trigger should be:

**For the side that just triggered (first leg)**:
- The trigger price is already known — it's the level that was just touched
- Record as P1_points

**For the opposite side (hunting target)**:
- Start with opposite side's Reference_points
- Subtract S0_points
- Apply RoundToTick
- Clamp to valid range [tick_size, 99]
- This becomes the trigger level for the opposite side

**Pair cost constraint enforcement**:
- Calculate OppositeMax_points = PairCap_points - P1_points
- Apply RoundToTick
- Opposite trigger = minimum(calculated_from_reference, OppositeMax_points)

This ensures the pair cost never exceeds PairCap.

---

## 7. SAMPLING AND MEASUREMENT CYCLES

### 7.1 How Cycles Work

Instead of evaluating triggers on every WebSocket event, the bot runs measurement cycles on a **fixed schedule**. Between cycles, it still receives and processes WebSocket events to maintain an accurate local orderbook state — but trigger evaluation only happens at scheduled cycle times.

### 7.2 Cycle Scheduling

**FIXED_INTERVAL mode**:
- Config specifies interval in seconds (e.g., `cycle_interval_seconds: 10`)
- Bot calculates: `remaining_time / interval = number_of_cycles`
- If 15 minutes remain: 900s / 10s = 90 cycles
- If 7 minutes remain: 420s / 10s = 42 cycles

**FIXED_COUNT mode**:
- Config specifies target count (e.g., `cycles_per_market: 30`)
- Bot calculates: `remaining_time / count = interval`
- If 15 minutes remain: 900s / 30 = one cycle every 30s
- If 7 minutes remain: 420s / 30 = one cycle every 14s

### 7.3 Auto-Adjustment for Late Joins

When the bot connects to a market that's already partially elapsed:
1. Calculate `time_remaining = settlement_time - now`
2. Based on the configured mode (interval or count), calculate the appropriate schedule
3. Start the first cycle immediately (captures the current state)
4. Schedule remaining cycles evenly across the remaining time
5. The last cycle should be scheduled a few seconds before settlement (not after)

### 7.4 What Happens at Each Cycle

1. **Capture snapshot**: Read current local orderbook state (maintained by WebSocket)
2. **Validate data**: Ensure both sides have bid/ask data; check for feed gaps
3. **Evaluate NEW triggers**: Calculate reference prices and trigger levels for YES and NO using current snapshot; check if ask <= trigger on either side
4. **Start new attempts**: If trigger conditions are met, create new attempt records
5. **Update active attempts**: For ALL currently active attempts, check if their opposite side has now triggered
6. **Record to DB**: Write snapshot and any state changes
7. **Update console**: Refresh dashboard display

---

## 8. TRIGGER EVALUATION

### 8.1 The PriceTouches Determination

For each measurement cycle snapshot:

**When trigger_rule is ASK_TOUCH**:

For YES (Up) side:
- Retrieve best ask price for YES in points from current snapshot
- Calculate YES trigger level: RoundToTick(YES_midpoint - S0)
- If best ask <= trigger level: condition is TRUE

For NO (Down) side:
- Retrieve best ask price for NO in points from current snapshot
- Calculate NO trigger level: RoundToTick(NO_midpoint - S0)
- If best ask <= trigger level: condition is TRUE

**Why "ask <= trigger"**: You want to BUY at your trigger price or better. The ask represents what sellers accept. If their ask is at or below your trigger, your limit order would fill.

### 8.2 Cycle-Only Evaluation

Trigger evaluation happens ONLY at scheduled cycle times. Do not evaluate between cycles even though WebSocket data arrives continuously. The WebSocket data maintains the local orderbook state; the cycles are when you actually check that state against trigger conditions.

---

## 9. STATE MACHINE SPECIFICATION

### 9.1 System States

**ACTIVE**: Market is open. Scheduled cycles are running. New attempts can start. Active attempts are monitored.

**SETTLED**: Settlement time reached. No new cycles. All remaining active attempts marked as failed.

### 9.2 Attempt Tracking Structure

Each active attempt is stored as an independent record:

- **attempt_id**: Unique identifier
- **first_leg_side**: YES or NO (which side triggered first)
- **P1_points**: Price at which first side triggered
- **t1_timestamp**: When first side triggered (cycle time)
- **reference_yes_points**: YES reference price at t1
- **reference_no_points**: NO reference price at t1
- **opposite_max_points**: Maximum price for opposite side
- **opposite_trigger_points**: Actual trigger price for opposite side (min of calculated and max)
- **status**: "active", "completed_paired", or "completed_failed"
- **t2_timestamp**: When second side triggered (null if not yet paired)

### 9.3 Main Processing Loop

**Initialization when market monitoring starts**:
1. Set market status to ACTIVE
2. Create empty list for active attempts
3. Initialize counters: total_attempts = 0, total_pairs = 0
4. Calculate cycle schedule based on time remaining and config mode
5. Start WebSocket subscription for market tokens

**At each scheduled cycle**:

**Step 1** — Check if settlement time reached. If so, process settlement and stop.

**Step 2** — Capture current orderbook snapshot from local state.

**Step 3** — Check for new YES trigger:
- YES_reference = (YES_bid + YES_ask) / 2
- YES_trigger = RoundToTick(YES_reference - S0)
- If YES_ask <= YES_trigger: start new attempt with first_leg = YES

**Step 4** — Check for new NO trigger:
- NO_reference = (NO_bid + NO_ask) / 2
- NO_trigger = RoundToTick(NO_reference - S0)
- If NO_ask <= NO_trigger: start new attempt with first_leg = NO

**Step 5** — If both triggered simultaneously: apply tie_break_rule for ordering, but create BOTH attempts.

**Step 6** — Update all active attempts: for each, check if opposite side has triggered in current snapshot.

**Step 7** — Clean up completed attempts from active tracking (keep in DB).

### 9.4 Starting a New Attempt

1. Increment total_attempts, assign attempt_id
2. Capture reference prices from current snapshot
3. Sanity check: abs((ref_yes + ref_no) - 100) <= 2
4. Record first leg: side, P1_points, t1_timestamp
5. Calculate opposite side trigger:
   - opposite_trigger_from_ref = RoundToTick(opposite_reference - S0), clamped to [tick_size, 99]
   - opposite_max = RoundToTick(PairCap - P1_points)
   - opposite_trigger = min(opposite_trigger_from_ref, opposite_max)
6. Create attempt record in DB with status = "active"
7. Log attempt start

### 9.5 Updating an Active Attempt

For each active attempt at each cycle:
1. Get opposite side's current best ask
2. If ask <= opposite_trigger_points:
   - Set t2_timestamp, calculate time_to_pair
   - pair_cost = P1 + actual_opposite_price
   - pair_profit = 100 - pair_cost
   - Status = "completed_paired", increment total_pairs
   - Update DB, log success
3. Else: keep active, move to next attempt

### 9.6 Settlement Processing

When settlement time is reached:
1. Mark ALL active attempts as "completed_failed" with fail_reason = "settlement_reached"
2. Calculate time_active for each failed attempt
3. Compute final stats: total_attempts, total_pairs, total_failed, pair_rate
4. Update Markets table with summary
5. Unsubscribe from WebSocket for this market's tokens
6. Log final market report

---

## 10. DATA MODEL SPECIFICATION

### 10.1 Markets Table (One Row Per Market Run)

| Field | Type | Description |
|-------|------|-------------|
| market_id | TEXT PK | Unique identifier (e.g., "btc-updown-15m-1768502700") |
| crypto_asset | TEXT | "btc", "eth", "sol", or "xrp" |
| condition_id | TEXT | Polymarket condition ID |
| yes_token_id | TEXT | Up token ID (long string) |
| no_token_id | TEXT | Down token ID (long string) |
| start_time | TEXT | ISO timestamp when monitoring began |
| settlement_time | TEXT | ISO timestamp when market resolves |
| actual_settlement_time | TEXT | When settlement was actually processed |
| tick_size_points | INTEGER | Tick size in points |
| parameter_set_id | INTEGER FK | Link to ParameterSets |
| total_attempts | INTEGER | Final attempt count |
| total_pairs | INTEGER | Successful pair count |
| total_failed | INTEGER | Failed attempt count |
| settlement_failures | INTEGER | Attempts still active at settlement |
| pair_rate | REAL | total_pairs / total_attempts |
| avg_time_to_pair | REAL | Average seconds for successful pairs |
| median_time_to_pair | REAL | Median seconds |
| max_concurrent_attempts | INTEGER | Peak simultaneous active attempts |
| total_cycles_run | INTEGER | How many measurement cycles were executed |
| cycle_interval_seconds | REAL | Actual interval between cycles |
| time_remaining_at_start | REAL | Seconds remaining when monitoring began |
| anomaly_count | INTEGER | Data quality issues flagged |
| notes | TEXT | Any anomalies or special conditions |

### 10.2 ParameterSets Table

| Field | Type | Description |
|-------|------|-------------|
| parameter_set_id | INTEGER PK | Auto-increment |
| name | TEXT | Human-readable name (e.g., "baseline") |
| S0_points | INTEGER | Initial spread offset |
| delta_points | INTEGER | Profit margin requirement |
| PairCap_points | INTEGER | 100 - delta |
| trigger_rule | TEXT | "ASK_TOUCH" |
| reference_price_source | TEXT | "MIDPOINT" or "LAST_TRADE" |
| tie_break_rule | TEXT | Description of cascade |
| sampling_mode | TEXT | "FIXED_INTERVAL" or "FIXED_COUNT" |
| cycle_interval_seconds | REAL | For FIXED_INTERVAL mode |
| cycles_per_market | INTEGER | For FIXED_COUNT mode |
| feed_gap_threshold_seconds | REAL | Gap detection threshold |
| created_at | TEXT | ISO timestamp |

### 10.3 Attempts Table (One Row Per Attempt)

| Field | Type | Description |
|-------|------|-------------|
| attempt_id | INTEGER PK | Auto-increment |
| market_id | TEXT FK | Link to Markets |
| parameter_set_id | INTEGER FK | Link to ParameterSets |
| cycle_number | INTEGER | Which cycle triggered this attempt |
| t1_timestamp | TEXT | When first side triggered |
| first_leg_side | TEXT | "YES" or "NO" |
| P1_points | INTEGER | First side trigger price |
| reference_yes_points | INTEGER | YES reference at t1 |
| reference_no_points | INTEGER | NO reference at t1 |
| opposite_side | TEXT | "YES" or "NO" |
| opposite_trigger_points | INTEGER | Trigger level for opposite |
| opposite_max_points | INTEGER | Max price from pair constraint |
| status | TEXT | "active", "completed_paired", "completed_failed" |
| t2_timestamp | TEXT | When second side triggered (null if failed) |
| t2_cycle_number | INTEGER | Which cycle completed the pair (null if failed) |
| time_to_pair_seconds | REAL | t2 - t1 in seconds (null if failed) |
| time_remaining_at_start | REAL | Seconds until settlement at t1 |
| time_remaining_at_completion | REAL | Seconds remaining when ended |
| actual_opposite_price | INTEGER | Ask price that triggered opposite (null if failed) |
| pair_cost_points | INTEGER | P1 + actual_opposite (null if failed) |
| pair_profit_points | INTEGER | 100 - pair_cost (null if failed) |
| fail_reason | TEXT | "settlement_reached" or null |
| had_feed_gap | INTEGER | Boolean: feed gap during attempt lifetime |

### 10.4 Snapshots Table (Optional — For Diagnostics)

| Field | Type | Description |
|-------|------|-------------|
| snapshot_id | INTEGER PK | Auto-increment |
| market_id | TEXT FK | Link to Markets |
| cycle_number | INTEGER | Which cycle captured this |
| timestamp | TEXT | ISO timestamp |
| yes_bid_points | INTEGER | Best bid for YES |
| yes_ask_points | INTEGER | Best ask for YES |
| no_bid_points | INTEGER | Best bid for NO |
| no_ask_points | INTEGER | Best ask for NO |
| yes_last_trade_points | INTEGER | Recent YES trade price |
| no_last_trade_points | INTEGER | Recent NO trade price |
| time_remaining | REAL | Seconds until settlement |
| active_attempts_count | INTEGER | Active attempts at this moment |
| anomaly_flag | INTEGER | Boolean for data quality issues |

### 10.5 AttemptLifecycle Table (Optional — For Deep Analysis)

| Field | Type | Description |
|-------|------|-------------|
| lifecycle_id | INTEGER PK | Auto-increment |
| attempt_id | INTEGER FK | Link to Attempts |
| cycle_number | INTEGER | Which cycle this observation is from |
| checkpoint_timestamp | TEXT | ISO timestamp |
| seconds_since_start | REAL | Time since attempt t1 |
| opposite_best_ask | INTEGER | Opposite side ask at this moment |
| distance_to_trigger | INTEGER | opposite_ask - opposite_trigger |
| closest_approach_so_far | INTEGER | Min distance seen so far |

---

## 11. EDGE CASE RESOLUTIONS

### 11.1 Feed Gaps

**Detection**: Time between consecutive WebSocket messages exceeds `feed_gap_threshold_seconds` (default: 10s).

**Action**:
- Log feed gap event with start, end, duration
- Do NOT assume any price touches occurred during gap
- Do NOT start new attempts based on stale data
- Active attempts: mark `had_feed_gap = true`, continue monitoring
- If gap overlaps a scheduled cycle: skip that cycle, log it, do not evaluate

### 11.2 Simultaneous Triggers

Both YES and NO trigger at the same cycle.

**Resolution**:
1. Calculate distance: trigger_points - current_ask_points for each side
2. Start attempt for side with smaller distance first
3. If equal, start YES first
4. **BOTH attempts are always created** — tie-break only determines ordering

### 11.3 Trigger Price Clamping

- **Lower bound**: If trigger < tick_size, clamp to tick_size. Log "trigger_clamped_to_min".
- **Upper bound**: If trigger > 99, clamp to 99. Log "trigger_clamped_to_max".

### 11.4 OppositeMax Exceeding 100

Log as "ERROR_IMPOSSIBLE_OPPOSITEMAX" — this is a bug detector, should never occur.

### 11.5 OppositeMax Below Tick Size

Set opposite_trigger = tick_size. Log "pair_constraint_impossible". Attempt still tracked but unlikely to pair.

### 11.6 Reference Price Sum Deviation

If ref_yes + ref_no deviates from 100 by > 2 points: log "reference_sum_anomaly", continue with calculated values, flag attempt.

### 11.7 Rapid Repeated Triggers

Same side triggers at consecutive cycles: each creates a separate independent attempt. All track simultaneously.

### 11.8 Attempt Starting Near Settlement

Create normally. It will almost certainly fail at settlement — that's correct behavior and valid data.

### 11.9 Empty Orderbook

If either side has no bids or no asks during a cycle: skip trigger evaluation for that cycle, log "orderbook_empty", do not start new attempts. Active attempts still monitored on next cycle.

### 11.10 WebSocket Disconnect

- Auto-reconnect with exponential backoff
- Resubscribe to all active market token IDs
- Log the disconnection period as a feed gap
- If disconnected for > 60 seconds, consider falling back to REST polling for active markets

---

## 12. MARKET DISCOVERY AND ROTATION

### 12.1 Continuous Operation

The bot runs 24/7, continuously monitoring all active 15-minute crypto markets across BTC, ETH, SOL, and XRP. As one market settles, it automatically transitions to the next market window for that asset.

### 12.2 Discovery Loop

Every 60 seconds:
1. Query Gamma API for all active markets matching `*-updown-15m-*`
2. For each configured crypto asset, identify the currently active market
3. If a new market is found that isn't currently monitored: start a new MarketMonitor
4. If a monitored market is no longer active: process settlement if not already done

### 12.3 Pre-Discovery

Before the current market settles, proactively look for the next market:
- When current market has < 2 minutes remaining, start checking for the next window
- The next market's slug should have a timestamp ~15 minutes after the current one
- Pre-create the MarketMonitor but don't start cycles until the current one settles (or the new market becomes active)

### 12.4 Market Transition

When a market settles:
1. Finalize all attempts for the settled market
2. Write market summary to DB
3. Tear down WebSocket subscriptions for settled market's tokens
4. If next market is already discovered and active: immediately start monitoring
5. If next market isn't found yet: keep polling Gamma API until it appears

### 12.5 Multi-Asset Parallel

All four assets (BTC, ETH, SOL, XRP) are monitored simultaneously. Each asset has its own independent chain of markets. They share the same parameter set but are tracked separately.

---

## 13. OUTPUT METRICS AND ANALYSIS

### 13.1 Core Metrics (Per Market)

**Primary**: attempts, pairs, failed, pair_rate, settlement_failures

**Timing**: avg/median/min/max/stddev of time_to_pair, percentile distribution (10th, 25th, 50th, 75th, 90th)

**Distribution**: time_to_pair histogram (0-10s, 10-30s, 30-60s, 60-120s, 120-300s, 300s+), pair cost distribution, profit margin distribution

**Concurrency**: max and avg concurrent attempts

### 13.2 Segmented Analysis

**By First Leg**: YES-first vs NO-first — count, pair rate, avg time-to-pair

**By Market Phase**: Early (first 5 min), Middle (middle 5 min), Late (final 5 min)

**By Reference Price Regime**: Balanced (45-55), YES-favored (56-70), NO-favored (30-44), Extreme (outside 30-70)

**By Crypto Asset**: Compare BTC vs ETH vs SOL vs XRP pair rates and timing

**By Pair Cost**: Cheap (<90), Medium (90-95), Expensive (>95)

### 13.3 Failure Analysis

- Proximity: how close did failed attempts get to triggering?
- Time: how long were failures active?
- Categories: near misses (within 2 pts), moderate (within 5), far (>10)

### 13.4 Aggregate Analysis (Across Markets)

After multiple markets with same parameters:
- Variance and consistency of pair rate
- Confidence intervals
- Profitability projections:
  - Breakeven pair rate = L / (profit_avg + L)
  - Expected value per attempt = R × profit_avg - (1-R) × L
  - Projected daily/monthly profit

### 13.5 Console Dashboard

While running, display a live console summary refreshed each cycle:

```
╔══════════════════════════════════════════════════════════════╗
║  POLYMARKET PAIR MEASUREMENT BOT — LIVE                      ║
║  Running since: 2026-02-05 08:00:00 UTC                     ║
║  Parameter set: baseline (S0=5, δ=3)                        ║
╠══════════════════════════════════════════════════════════════╣
║  MARKET          │ TIME LEFT │ CYCLE │ ATTEMPTS │ PAIRS │ % ║
║  btc-15m-...700  │   8m 32s  │ 22/90 │    14    │   6   │43%║
║  eth-15m-...700  │   8m 32s  │ 22/90 │     9    │   3   │33%║
║  sol-15m-...700  │   8m 32s  │ 22/90 │    11    │   5   │45%║
║  xrp-15m-...700  │   8m 32s  │ 22/90 │     7    │   2   │29%║
╠══════════════════════════════════════════════════════════════╣
║  SESSION TOTALS: Markets: 12 | Attempts: 284 | Pairs: 119  ║
║  Overall pair rate: 41.9% | Avg time-to-pair: 34.2s        ║
╚══════════════════════════════════════════════════════════════╝
```

---

## 14. VALIDATION AND SANITY CHECKS

### 14.1 Pre-Run Validation

Before processing:
- All config parameters present and valid
- S0 > 0 and < 50
- Delta > 0 and < 50
- Cycle interval > 0 (or count > 0)
- Database accessible and tables exist
- Polymarket API reachable (GET /time succeeds)
- At least one crypto asset configured

### 14.2 Runtime Sanity Checks

**Per Cycle**:
- Reference price sum within 100 ± 2
- Trigger prices between tick_size and 99
- OppositeMax between 0 and 100
- Best bid <= best ask on each side
- Timestamps monotonically increasing

**Per Attempt Creation**:
- attempt_id unique
- All required fields populated
- Reference prices pass sanity check
- Opposite trigger in valid range

**System Health**:
- Active attempts list not growing unbounded
- WebSocket connection alive
- DB writes succeeding
- Feed gap detection working

If anomaly_count > 50 per market, flag as potentially unreliable.

### 14.3 Post-Run Validation (Per Market)

- pair_count <= attempts_count
- All attempts have terminal status
- Settlement failures = attempts active at settlement
- time_to_pair values positive and < market duration
- Pair costs <= PairCap
- All t2 > t1

---

## 15. KNOWN LIMITATIONS

### What This Measures
- Frequency of price touches at trigger levels
- How often both sides touch before settlement
- Timing of mean reversion
- Market regime impact on pairing

### What This Does NOT Measure
- Order execution latency
- Partial fills
- Market impact
- Competitive dynamics (other bots)
- Gas fees and transaction costs
- Slippage on exits
- API rate limit impact
- Queue position in order book

### Assumptions
- Limit order at trigger price fills when ask touches trigger
- Full position available at trigger price
- Market data feed is accurate
- Infinite capital (no inventory constraint)
- All opportunities are independent

### Over-Counting Risk
Overlapping attempts count more opportunities than live trading (with single-position constraint) could capture. **Adjustment factor**: expect live trading to capture 50-75% of measured pairs.

### Under-Counting Risk
Cycle-based sampling misses price touches between cycles. A 10-second cycle interval could miss a sub-second price dip. This is a known trade-off of the fixed-schedule design.

### Conservative Live Translation
Measured pair rate → live pair rate adjustments:
- Inventory constraint: × 0.60
- Execution lag: × 0.85
- Competition: × 0.75
- **Need measured pair rate > 50% to survive adjustments and remain profitable**

---

## 16. REPOSITORY STRUCTURE

```
polymarket-pair-measure/
├── README.md                        # Project overview and setup instructions
├── requirements.txt                 # Python dependencies
├── config.yaml                      # Parameter sets, feature flags, asset list
├── src/
│   ├── __init__.py
│   ├── main.py                      # Entry point, orchestrator, asyncio event loop
│   ├── config.py                    # Load/validate config.yaml
│   ├── models.py                    # Dataclasses: Attempt, MarketState, Snapshot, etc.
│   ├── database.py                  # SQLite schema creation, async read/write
│   ├── market_discovery.py          # Gamma API polling, market filtering, pre-discovery
│   ├── market_monitor.py            # Per-market: WS subscription, cycle scheduling, state
│   ├── trigger_evaluator.py         # Core logic: reference calc, trigger check, attempt mgmt
│   ├── price_utils.py               # Point conversion, tick rounding, Decimal handling
│   ├── websocket_client.py          # WS connection, reconnect, heartbeat, event parsing
│   ├── rest_client.py               # CLOB REST wrapper (fallback + validation)
│   ├── settlement.py                # Settlement detection and attempt finalization
│   ├── metrics.py                   # Post-run analysis queries
│   ├── dashboard.py                 # Console display (live stats)
│   └── logging_config.py            # Structured logging setup
├── scripts/
│   ├── analyze_results.py           # Run aggregate analysis on collected data
│   └── export_data.py               # Export to CSV
└── data/
    └── .gitkeep                     # measurements.db created at runtime (gitignored)
```

---

## 17. CONFIGURATION

```yaml
# config.yaml

parameter_sets:
  - name: "baseline"
    S0_points: 5
    delta_points: 3
    trigger_rule: "ASK_TOUCH"
    reference_price_source: "MIDPOINT"

sampling:
  mode: "FIXED_INTERVAL"          # or "FIXED_COUNT"
  cycle_interval_seconds: 10      # for FIXED_INTERVAL mode
  cycles_per_market: 90           # for FIXED_COUNT mode

markets:
  crypto_assets: ["btc", "eth", "sol", "xrp"]
  market_type: "15m"              # 15-minute markets only for v1
  discovery_poll_interval_seconds: 60
  pre_discovery_lead_seconds: 120  # start looking for next market 2 min before settlement

data:
  database_path: "data/measurements.db"
  enable_snapshots: false          # store raw cycle snapshots
  enable_lifecycle_tracking: false # store per-cycle attempt state

quality:
  feed_gap_threshold_seconds: 10
  max_reference_sum_deviation: 2
  enable_sanity_checks: true
  max_anomalies_per_market: 50

logging:
  level: "INFO"
  file: "logs/bot.log"
  console_dashboard: true

websocket:
  url: "wss://ws-subscriptions-clob.polymarket.com/ws/market"
  heartbeat_interval_seconds: 30
  reconnect_max_delay_seconds: 60
  rest_fallback_after_disconnect_seconds: 60
```

---

## 18. BUILD PHASES

### PHASE 1 — Core Engine (BUILD THIS NOW)

Build the foundational system that can monitor ONE market end-to-end:

1. **Config loader** (`config.py`): Parse config.yaml, validate parameters
2. **Price utilities** (`price_utils.py`): Point conversion, tick rounding, Decimal math
3. **Data models** (`models.py`): Dataclasses for Attempt, MarketState, Snapshot
4. **Database** (`database.py`): Create SQLite tables, async write functions
5. **REST client** (`rest_client.py`): Wrapper for CLOB REST endpoints (GET /book, /midpoint, /time)
6. **Market discovery** (`market_discovery.py`): Query Gamma API, find active 15-min markets, extract token IDs
7. **WebSocket client** (`websocket_client.py`): Connect, subscribe, parse events, maintain local orderbook, reconnect
8. **Trigger evaluator** (`trigger_evaluator.py`): The core — reference calc, trigger check, attempt creation, attempt updates, settlement
9. **Market monitor** (`market_monitor.py`): Ties it together — manages one market's lifecycle with scheduled cycles
10. **Main** (`main.py`): Entry point — discover one BTC market, create monitor, run until settlement, print summary
11. **Logging** (`logging_config.py`): File + console logging

**Phase 1 success criteria**: Run the bot, it connects to a live BTC 15-min market, runs measurement cycles, creates attempts when triggers are met, tracks pairs, processes settlement, writes results to SQLite, and prints a summary.

### PHASE 2 — Multi-Market and Rotation (LATER)

- Multi-asset parallel monitoring (BTC + ETH + SOL + XRP simultaneously)
- Continuous market rotation (auto-transition to next market window)
- Pre-discovery of upcoming markets
- Graceful handling of overlapping market lifecycles

### PHASE 3 — Dashboard and Analysis (LATER)

- Live console dashboard with per-market stats
- Analysis queries and reporting (`metrics.py`, `analyze_results.py`)
- CSV export
- Parameter sweep support (multiple parameter sets per run)
- Failure proximity analysis

---

## RECOMMENDED INITIAL PARAMETER VALUES

For first test run:

| Parameter | Value | Reasoning |
|-----------|-------|-----------|
| S0_points | 5 | Trigger 5 points ($0.05) below reference |
| delta_points | 3 | Require 3% profit margin, PairCap = 97 |
| trigger_rule | ASK_TOUCH | Matches real order execution |
| reference_price_source | MIDPOINT | More stable than last trade |
| sampling mode | FIXED_INTERVAL | Simpler to reason about |
| cycle_interval_seconds | 10 | 90 cycles per full 15-min market |
| feed_gap_threshold | 10 | Flag gaps > 10 seconds |

---

## EXAMPLE SQL QUERIES FOR ANALYSIS

### Basic Pair Rate
```sql
SELECT parameter_set_id,
       COUNT(*) as total_attempts,
       SUM(CASE WHEN status = 'completed_paired' THEN 1 ELSE 0 END) as pairs,
       AVG(CASE WHEN status = 'completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
FROM Attempts
WHERE parameter_set_id = ?
GROUP BY parameter_set_id;
```

### Time-to-Pair Distribution
```sql
SELECT
  CASE
    WHEN time_to_pair_seconds < 10 THEN '0-10s'
    WHEN time_to_pair_seconds < 30 THEN '10-30s'
    WHEN time_to_pair_seconds < 60 THEN '30-60s'
    WHEN time_to_pair_seconds < 120 THEN '60-120s'
    ELSE '120s+'
  END as bucket,
  COUNT(*) as count,
  AVG(pair_profit_points) as avg_profit
FROM Attempts
WHERE status = 'completed_paired' AND parameter_set_id = ?
GROUP BY bucket
ORDER BY MIN(time_to_pair_seconds);
```

### By Crypto Asset
```sql
SELECT m.crypto_asset,
       COUNT(*) as attempts,
       AVG(CASE WHEN a.status = 'completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
       AVG(a.time_to_pair_seconds) as avg_ttp
FROM Attempts a
JOIN Markets m ON a.market_id = m.market_id
GROUP BY m.crypto_asset;
```

### Cross-Market Consistency
```sql
SELECT market_id,
       COUNT(*) as attempts,
       AVG(CASE WHEN status = 'completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
FROM Attempts
WHERE parameter_set_id = ?
GROUP BY market_id
ORDER BY pair_rate DESC;
```

### Market Phase Analysis
```sql
SELECT
  CASE
    WHEN time_remaining_at_start > 600 THEN 'Early (10min+)'
    WHEN time_remaining_at_start > 300 THEN 'Middle (5-10min)'
    ELSE 'Late (0-5min)'
  END as phase,
  COUNT(*) as attempts,
  AVG(CASE WHEN status = 'completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate
FROM Attempts
WHERE parameter_set_id = ?
GROUP BY phase;
```
