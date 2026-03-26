# Optimization Pipeline Audit

A step-by-step analysis of the daily optimization report, examining whether each stage correctly measures what we think it does and whether the results are indicative of real trading performance.

---

## How the Bot Actually Trades (for context)

The bot runs parameter sets defined by env vars: `(S0_points, delta_points, stop_loss_threshold_points)`. For each market, it:
1. Places a first leg at price P1 (in points)
2. Waits for the market to move `delta_points` in its favor → "paired" (win)
3. If the market moves against it to `stop_loss_threshold_points` → "failed" (loss), or if it never pairs, the attempt fails at P1

**What the optimizer is trying to do**: Find the best combination of `(delta, stop_loss, P1 range, time_remaining range)` that would have been most profitable historically.

---

## Stage 1: Grid Aggregation

### What it does
Queries the `attempt_stats` table — a pre-aggregated table with columns `(delta_points, stop_loss_threshold_points, P1_points, time_minute, crypto_asset, attempt_date, status, ...)`. Groups by `(delta, SL, P1, time_minute)` and sums attempts/pairs/total_pnl.

### Assessment: Generally Sound, but...

**Problem 1: The grid aggregates ALL (delta, SL) combos the bot has ever run.** If the bot ran `delta=3` for 2 weeks and `delta=7` for 2 months, the grid contains both — but the `delta=7` data is 4x larger and will dominate any box search. The optimizer may recommend configs that were only profitable during a specific deployment period, not because the parameters are inherently better.

**Problem 2: P1 and time_remaining are NOT parameters we control.** `P1` is the market price when we enter, and `time_remaining` is how much time is left on the market. These are *market conditions*, not trading parameters. The optimizer treats them as tunable knobs, but in reality we can only *filter* on them (refuse to enter if P1 or time are outside a range). This distinction matters because:
- Optimizing "which P1 range was profitable" is really asking "which market conditions were favorable" — this may not persist
- The bot currently does NOT filter on P1 or time_remaining in production (unless `FIRST_LEG_MIN_PRICE_POINTS` / `ENTRY_WINDOW_*` are set)

**Verdict**: The grid itself is fine mechanically, but it conflates *parameter selection* (delta, SL) with *market condition filtering* (P1, time). These should be evaluated differently.

---

## Stage 2: 2D Prefix-Sum Box Search

### What it does
For each `(delta, SL)` combination, builds a 2D matrix of `(P1, time_minute)` cells and searches ALL possible rectangular boxes. For each box, computes:
- `pair_rate = pairs / attempts`
- `avg_pnl = total_pnl / attempts`
- `best_g = max over fractions of: p * ln(1 + f*delta/100) + (1-p) * ln(1 - f*loss/100)`

Filters: `min_attempts >= 200`, `min_box_days >= 14`, `avg_pnl > 0`, `best_g > 0`.

### Assessment: THIS IS THE MOST PROBLEMATIC STAGE

**Problem 3: Massive multiple comparisons / data snooping.** The box search enumerates *every possible* rectangular region in a 2D grid. For a typical grid with ~50 P1 values and ~30 time values, that's:
- C(50,2) * C(30,2) = 1,225 * 435 = **533,000+ boxes per (delta, SL) combo**
- With ~10 (delta, SL) combos = **5+ million hypothesis tests**

With 5 million tests, you will *always* find boxes that look profitable by chance. The `min_attempts >= 200` filter helps but doesn't solve this. A 200-attempt sample with a true 50% pair rate will show pair rates ranging from ~43% to ~57% just from sampling noise. At the extremes, this makes configs look profitable when they aren't.

**Problem 4: The analytical g-proxy uses the midpoint P1 for loss calculation.** The formula uses `p1_mid = (p1_lo + p1_hi) / 2` as the loss for failed attempts. But actual losses vary across the P1 range — attempts at P1=45 lose 45 points, attempts at P1=5 lose 5 points. Using the midpoint smooths over the heterogeneity within the box. A box might look good at the midpoint but have a bimodal distribution of outcomes (many small wins at low P1, many large losses at high P1).

**Problem 5: The box search implicitly assumes stationarity.** Finding that "P1 in [20-30] was profitable over the last 7 days" assumes this relationship will hold going forward. But P1 distributions shift with market regime — during high-volatility periods, P1 clusters differently than during low-volatility periods. The 14-day minimum span helps somewhat, but a 7-day optimization window (our new default) makes this worse.

**Problem 6: Time_minute as a predictor is suspect.** `time_remaining_at_start` reflects when the bot entered relative to market expiry. Finding that "entries with 10-20 minutes remaining were profitable" could be:
- A real signal (markets behave differently near expiry)
- An artifact of when the bot happened to be running
- Correlated with a specific event (e.g., one big market that expired at a certain time)

**Verdict**: The box search is the core weakness. It's a brute-force search over millions of hypotheses with no correction for multiple testing. The results are heavily biased toward finding spurious patterns, especially with a 7-day window.

---

## Stage 3: Bootstrap Compound Bankroll Simulation

### What it does
For the top 100 configs from Stage 2:
1. Fetches actual market outcomes with `DISTINCT ON (market_id)` — one outcome per market
2. Simulates compounding: `bankroll *= (1 + f * delta/100)` for wins, `bankroll *= (1 - f * loss/100)` for losses
3. Bootstrap resampling (5000x) for confidence intervals
4. Ranks by `proxy_g` (analytical, not bootstrapped) — this is good

### Assessment: Mechanically Correct but Built on Stage 2's Flawed Foundation

**Problem 7: DISTINCT ON (market_id) changes the unit of analysis.** Stage 2 counts *attempts* (multiple per market). Stage 3 deduplicates to one outcome per *market*. This means:
- A config might show 500 attempts in Stage 2, but only 80 distinct markets in Stage 3
- The pair rate in Stage 2 (attempt-level) can differ significantly from the pair rate in Stage 3 (market-level)
- The bankroll simulation in Stage 3 uses market-level outcomes, but the *ranking into Stage 3* was based on attempt-level metrics from Stage 2

This creates an inconsistency: configs are selected by one metric and evaluated by another.

**Problem 8: Bootstrap assumes i.i.d. outcomes.** Resampling with replacement assumes each market outcome is independent. In reality:
- Markets in the same crypto asset at the same time are correlated
- Macro events (BTC crash) affect all markets simultaneously
- Sequential markets may share regime (trending vs. mean-reverting)

The bootstrap CI is narrower than it should be, giving false confidence.

**Problem 9: Compound bankroll simulation is order-dependent but bootstrap destroys order.** The actual bankroll path depends on the *sequence* of wins/losses (a loss after a big win hurts less than a loss at the start). Bootstrap resampling shuffles order, computing many possible paths. This is fine for measuring *expected* log-growth, but the reported `final_bankroll` (non-bootstrapped) is just one specific historical ordering — it could be misleading if a lucky streak happened to come first.

**Verdict**: Stage 3 is the most defensible part of the pipeline. The ranking by analytical `proxy_g` (not bootstrapped) avoids sample-size inflation. But it's only as good as the configs fed to it from Stage 2.

---

## Walk-Forward Validation

### What it does (in daily report context)
Takes the top 5 configs from optimization and evaluates them on a rolling window over the last 30 days:
- 4-day windows, stepped by 1 day
- For each window: fetch attempts, dedup per market, compute bankroll
- Report per-window metrics and cross-window average

### Assessment: Good Idea, Weak Execution

**Problem 10: The walk-forward in the daily report is NOT true out-of-sample.** The optimization runs on the last 7 days. The walk-forward runs on the last 30 days. Since the 7-day optimization period is a *subset* of the 30-day walk-forward period, the walk-forward windows that overlap with the optimization period are in-sample, not out-of-sample. Approximately 7 out of 26 windows (~27%) are contaminated.

For a proper IS/OOS split:
- Optimize on days [-37, -30] (the 7 days before the walk-forward period)
- Walk-forward on days [-30, 0] (the validation period)
- OR: optimize on [-7, 0] and walk-forward on [-37, -7]

**Problem 11: 4-day windows with 5 configs means very few data points per evaluation.** A 4-day window might contain 10-30 market outcomes per config. With such small samples, per-window bankroll metrics are extremely noisy. The cross-window average is more stable, but 26 windows of 4 days each are highly overlapping (3 out of 4 days shared between adjacent windows), so they are not independent.

**Problem 12: Walk-forward doesn't test what matters most.** The key question isn't "did this config do well over the past 30 days?" but "will this config do well *tomorrow*?" The walk-forward shows trailing performance but doesn't measure predictive power. A config that was great for 25 out of 26 windows but collapsed in the most recent 5 is more concerning than one that was mediocre but stable.

**Verdict**: The walk-forward provides some value but gives a false sense of validation due to IS/OOS contamination and overlapping windows.

---

## Fundamental Issues

### Issue A: What are we actually optimizing?

The pipeline optimizes `(delta, stop_loss, P1_range, time_range)` jointly. But in production:
- `delta` and `stop_loss` are set as env vars and apply to ALL markets
- `P1_range` and `time_range` are market conditions — they're only actionable if we add entry filters

So the optimizer's top config might say "delta=5, SL=3, P1=20-35, time=8-15min" — but unless we configure the bot to reject entries outside those P1 and time ranges, we're running with delta=5/SL=3 across ALL P1 and time values, which may perform differently.

**The optimization result and actual production behavior are measuring different things.**

### Issue B: Points are not dollars

The entire pipeline operates in "points" (1 point = 1 cent in Polymarket odds). But actual P&L depends on:
- Position size (how many shares per attempt)
- How many concurrent markets the bot is active in
- Capital allocation across parameter sets

A config showing "+0.5 avg PNL per attempt" means +$0.005 per share. With 100 shares per market, that's $0.50 per attempt. The bankroll simulation assumes we reinvest 10% of bankroll on each bet, but the bot doesn't actually do Kelly sizing — it uses fixed position sizes.

**The compound bankroll simulation models a strategy we don't actually execute.**

### Issue C: Attempt-level aggregation hides market-level correlation

Stage 2 uses attempt-level statistics. Multiple attempts can happen on the same market (e.g., if the bot retries after a failed attempt). This inflates the apparent sample size — 500 attempts might come from 50 markets, and those 50 markets' outcomes are correlated (same underlying event).

Stage 3 correctly deduplicates to market-level, but the *selection* into Stage 3 was based on inflated attempt-level statistics.

### Issue D: The 7-day optimization window is very short

With the new default of 7 days:
- Fewer markets expire in 7 days → smaller sample sizes
- Higher sensitivity to one-off events (a single volatile day can dominate)
- More likely to find spurious patterns in the box search
- The `min_box_days >= 14` filter means configs must span at least 14 calendar days, but with `--days 7` the grid only contains 7 days of data, so this filter is effectively `min_box_days >= 7` (capped by data availability)

Wait — actually, `box_days` is computed from the `min_ts` and `max_ts` of the actual data in the box. If we only have 7 days of data, `box_days` can be at most ~7. The `box_days >= 14` filter would eliminate EVERYTHING, making the optimizer return no results.

**This is likely a bug introduced by our change from 30→7 day default.** The 14-day minimum span was calibrated for 30 days of data.

---

## Summary of Findings

| # | Issue | Severity | Category |
|---|-------|----------|----------|
| 1 | Grid mixes data from different deployment periods | Medium | Data quality |
| 2 | P1/time are market conditions, not tunable parameters | High | Conceptual |
| 3 | Millions of hypothesis tests with no correction | Critical | Statistical |
| 4 | G-proxy uses midpoint P1, not actual loss distribution | Low | Approximation |
| 5 | Box search assumes stationarity over optimization window | High | Statistical |
| 6 | Time_minute as predictor may be spurious | Medium | Statistical |
| 7 | Stage 2 uses attempts, Stage 3 uses markets (inconsistency) | Medium | Design |
| 8 | Bootstrap assumes i.i.d. (ignores market correlation) | Medium | Statistical |
| 9 | Single historical bankroll path can be misleading | Low | Reporting |
| 10 | Walk-forward overlaps with optimization period (IS contamination) | High | Validation |
| 11 | 4-day overlapping windows are not independent | Medium | Validation |
| 12 | Walk-forward measures trailing, not predictive, performance | Medium | Validation |
| A | Optimizer output doesn't match production behavior | Critical | Conceptual |
| B | Points ≠ dollars, compound sim ≠ actual sizing | High | Conceptual |
| C | Attempt-level aggregation inflates sample sizes | Medium | Statistical |
| D | 7-day window likely breaks the 14-day min_box_days filter | Critical | Bug |

---

---

## Polyforge: What Production Actually Does

The real trading bot (polyforge) is a TypeScript/Node.js app on AWS EC2. Understanding how it works reveals fundamental gaps between what we optimize and what we run.

### Production Config (ETH instance)

```yaml
delta: 0.14                    # 14 points
stopLossThreshold: 0.33        # 33 points
firstLegMinPrice: 0.76         # P1 filter: 76-80 cents
firstLegMaxPrice: 0.80
entryWindowStart: 13           # Enter 13-10 min before settlement
entryWindowEnd: 10
riskPercent: 0.10              # 10% of bankroll per position
maxPositionsPerCycle: 3
```

### Production Position Sizing

```typescript
R = bankroll * riskPercent     // e.g., $10,000 * 0.10 = $1,000
Q = R / (1 - delta)           // $1,000 / 0.86 = 1,162 shares
```

This is **NOT Kelly criterion**. It's a simple risk-per-position model where `R` is the max loss if only the first leg fills.

### How Production Differs from the Optimizer

| Aspect | Optimizer Assumes | Production Reality |
|--------|-------------------|-------------------|
| **P1 filtering** | Searches all P1 ranges as if they're tunable | Production DOES filter P1 (76-80 range) |
| **Time filtering** | Searches all time ranges as if they're tunable | Production DOES filter time (13-10 min window) |
| **Position sizing** | 10% Kelly-fractional compound reinvestment | 10% risk-per-position, NOT Kelly |
| **Bankroll simulation** | `bankroll *= (1 + f * delta/100)` per market | `Q = bankroll * 0.10 / (1 - delta)` per cycle |
| **Compounding model** | Multiplicative log-growth | Linear allocation with peak tracking & floor |
| **Loss calculation** | `-P1` or `-(SL + taker_fee)` in points | Market sell at current bid (actual slippage) |
| **Units** | Points (cents) | USDC with 6-decimal precision |
| **Concurrent positions** | One at a time (sequential simulation) | Up to 3 per cycle |
| **Stop loss execution** | Instant, at threshold price | Market order at next tick below threshold (slippage) |

### Critical Mismatch E: The Bankroll Simulation Doesn't Model Production

The optimizer simulates:
```python
bankroll *= (1 + 0.10 * 14 / 100)   # win: bankroll * 1.014
bankroll *= (1 - 0.10 * 33 / 100)   # loss: bankroll * 0.967
```

Production actually does:
```typescript
R = bankroll * 0.10                   // allocate 10% as risk capital
Q = R / (1 - 0.14)                   // buy Q shares at P1
// Win: profit = Q * delta = Q * 0.14 USDC (NOT 1.4% of bankroll)
// Loss: lose up to R (if stop loss at full threshold)
```

The key difference: in production, profit is `Q * delta` in absolute terms, not a percentage of bankroll. Since `Q = bankroll * 0.10 / 0.86`, the actual return per win is `bankroll * 0.10 * 0.14 / 0.86 = bankroll * 0.01628` (~1.6%), while the optimizer models it as `bankroll * 0.10 * 14/100 = bankroll * 0.014` (1.4%). These are close but not identical because the optimizer divides by 100 (points to dollars) while production divides by `(1 - delta)`.

More importantly, the optimizer models losses as `bankroll * 0.10 * loss_points / 100`, but production losses depend on the *actual market sell price*, which includes slippage. A stop loss at threshold=33 doesn't mean you lose exactly 33 points — you market-sell at whatever the bid is, which could be lower.

### Critical Mismatch F: Concurrent Positions Are Ignored

The optimizer simulates one bet at a time, sequentially. Production runs up to 3 positions per cycle (every 15 minutes). This means:
- Capital is split across positions (each gets `bankroll * 0.10` — so 30% of bankroll may be at risk simultaneously)
- Correlation between concurrent positions is ignored (if the market dumps, all 3 stop out)
- The optimizer's "pair rate" and "expected return" don't account for the portfolio effect

### Critical Mismatch G: P1 and Time ARE Filtered in Production (But Not How the Optimizer Thinks)

The optimizer was designed before polyforge added entry filters. The production config has:
- `firstLegMinPrice: 0.76, firstLegMaxPrice: 0.80` (hard P1 filter)
- `entryWindowStart: 13, entryWindowEnd: 10` (hard time filter)

So the optimizer's box search for the "best P1 range" is actually useful — BUT the optimizer searches in *points* (integer cents) while production uses *decimal prices* (0.76 = 76 points). More importantly, the optimizer's recommended ranges may not match what's currently deployed. If the optimizer says "P1=20-35 is best" but production runs P1=76-80, the optimization is irrelevant to what's actually trading.

The optimizer should either:
1. Evaluate the *currently deployed* parameters (not search for new ones), OR
2. Its recommendations should be explicitly mapped to production config changes

---

## Revised Summary of All Issues

| # | Issue | Severity | Category |
|---|-------|----------|----------|
| 1 | Grid mixes data from different deployment periods | Medium | Data quality |
| 2 | P1/time are market conditions, not tunable parameters | Medium | Conceptual (partially mitigated — production does filter) |
| 3 | Millions of hypothesis tests with no correction | Critical | Statistical |
| 4 | G-proxy uses midpoint P1, not actual loss distribution | Low | Approximation |
| 5 | Box search assumes stationarity over optimization window | High | Statistical |
| 6 | Time_minute as predictor may be spurious | Medium | Statistical |
| 7 | Stage 2 uses attempts, Stage 3 uses markets (inconsistency) | Medium | Design |
| 8 | Bootstrap assumes i.i.d. (ignores market correlation) | Medium | Statistical |
| 9 | Single historical bankroll path can be misleading | Low | Reporting |
| 10 | Walk-forward overlaps with optimization period (IS contamination) | High | Validation |
| 11 | 4-day overlapping windows are not independent | Medium | Validation |
| 12 | Walk-forward measures trailing, not predictive, performance | Medium | Validation |
| A | Optimizer output doesn't match production behavior | High | Conceptual |
| B | Points ≠ dollars, compound sim ≠ actual sizing | High | Conceptual |
| C | Attempt-level aggregation inflates sample sizes | Medium | Statistical |
| D | 7-day window likely breaks the 14-day min_box_days filter | Critical | Bug |
| E | Bankroll simulation doesn't model production math | High | Conceptual |
| F | Concurrent positions (up to 3) are ignored | High | Conceptual |
| G | Optimizer's P1/time ranges don't map to deployed config | High | Actionability |

---

## Recommendations

### Immediate Fixes

1. **Fix the min_box_days bug (Issue D)**: The 7-day optimization window breaks the hardcoded `min_box_days >= 14` filter. Either lower it proportionally or revert to 30 days.

2. **Fix IS/OOS contamination (Issue 10)**: The optimization and walk-forward windows overlap. Either:
   - Optimize on [-37, -30], validate on [-30, 0]
   - OR optimize on [-7, 0], validate on [-37, -7]

### Structural Improvements

3. **Add a "production config evaluation" mode**: Instead of searching for new configs, evaluate the *currently deployed* parameters against recent data. Answer: "How did delta=14, SL=33, P1=76-80, time=13-10 actually perform this week?" This is the most actionable report.

4. **Match the bankroll simulation to production math**: Replace the Kelly-style `bankroll *= (1 + f * delta/100)` with the actual production formula: `profit = bankroll * riskPercent * delta / (1 - delta)`. Account for concurrent positions.

5. **Add multiple-testing correction to box search**: Apply a Bonferroni/FDR correction, or use a permutation test to measure how often random data produces configs as good as the real ones.

6. **Separate parameter search from condition filtering**: Run two analyses:
   - Which `(delta, SL)` combo is best across all conditions?
   - For the best `(delta, SL)`, which `(P1, time)` entry filters improve performance?

7. **Use market-level metrics consistently**: Replace attempt-level pair_rate and avg_pnl in Stage 2 with market-level equivalents.

### Nice-to-Have

8. **Add regime detection**: Cluster data by volatility regime and report per-regime performance.

9. **Model stop-loss slippage**: Production stop losses execute at market bid, not at the threshold price. The optimizer should account for slippage.
