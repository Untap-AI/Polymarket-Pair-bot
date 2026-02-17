/**
 * Analytics queries – mirrors src/metrics.py from the Python bot.
 *
 * All functions accept FilterParams and return plain objects.
 * The postgres driver returns rows as plain JS objects already.
 */

/* eslint-disable @typescript-eslint/no-explicit-any */

import { getDb } from "./db";
import {
  FilterParams,
  buildWhere,
  needsMarketJoin,
  PRICE_REGIME_CASE,
  TIME_REMAINING_CASE,
  COMBINED_SPREAD_CASE,
} from "./filters";

// ---------------------------------------------------------------
// Helper: base FROM + optional JOIN
// ---------------------------------------------------------------

function baseFrom(filters: FilterParams): string {
  const join = needsMarketJoin(filters)
    ? " JOIN Markets m ON a.market_id = m.market_id"
    : "";
  return `FROM Attempts a${join}`;
}

// Always join Markets (for queries that always need it)
const FROM_WITH_MARKETS =
  "FROM Attempts a JOIN Markets m ON a.market_id = m.market_id";

// ---------------------------------------------------------------
// Filter option queries (for populating dropdowns)
// ---------------------------------------------------------------

export async function getFilterOptions() {
  const sql = getDb();

  const [deltas, s0Values, stopLosses, assets, paramSets] = await Promise.all([
    sql`SELECT DISTINCT delta_points FROM Attempts WHERE delta_points IS NOT NULL ORDER BY delta_points`,
    sql`SELECT DISTINCT S0_points FROM Attempts WHERE S0_points IS NOT NULL ORDER BY S0_points`,
    sql`SELECT DISTINCT stop_loss_threshold_points FROM ParameterSets WHERE stop_loss_threshold_points IS NOT NULL ORDER BY stop_loss_threshold_points`,
    sql`SELECT DISTINCT crypto_asset FROM Markets ORDER BY crypto_asset`,
    sql`SELECT parameter_set_id, name, delta_points, S0_points, stop_loss_threshold_points FROM ParameterSets ORDER BY parameter_set_id`,
  ]);

  return {
    deltaPoints: deltas.map((r) => r.delta_points as number),
    s0Values: s0Values.map((r) => r.s0_points as number),
    stopLossValues: stopLosses.map(
      (r) => r.stop_loss_threshold_points as number
    ),
    assets: assets.map((r) => (r.crypto_asset as string).toUpperCase()),
    parameterSets: paramSets.map((r) => ({
      id: r.parameter_set_id as number,
      name: r.name as string,
      delta: r.delta_points as number,
      s0: r.s0_points as number,
      stopLoss: r.stop_loss_threshold_points as number | null,
    })),
    priceRegimes: [
      "Balanced (45-55)",
      "YES-favored (56-70)",
      "NO-favored (30-44)",
      "Extreme (<30 or >70)",
    ],
    timeRemainingBuckets: [
      "15 min", "14 min", "13 min", "12 min", "11 min",
      "10 min", "9 min", "8 min", "7 min", "6 min",
      "5 min", "4 min", "3 min", "2 min", "1 min",
    ],
    combinedSpreadBuckets: [
      "Tight (<=2)",
      "Normal (3-4)",
      "Wide (5-6)",
      "Very wide (7+)",
    ],
    daysOfWeek: [
      { value: 0, label: "Sunday" },
      { value: 1, label: "Monday" },
      { value: 2, label: "Tuesday" },
      { value: 3, label: "Wednesday" },
      { value: 4, label: "Thursday" },
      { value: 5, label: "Friday" },
      { value: 6, label: "Saturday" },
    ],
  };
}

// ---------------------------------------------------------------
// Overall stats
// ---------------------------------------------------------------

export async function getOverallStats(filters: FilterParams) {
  const sql = getDb();
  const from = baseFrom(filters);
  const { clause, values } = buildWhere(filters);

  const query = `
    SELECT
      COUNT(*)::int as total_attempts,
      SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END)::int as total_pairs,
      SUM(CASE WHEN a.status='completed_failed' THEN 1 ELSE 0 END)::int as total_failed,
      SUM(CASE WHEN a.fail_reason='stop_loss' THEN 1 ELSE 0 END)::int as total_stopped,
      AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
      AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp,
      AVG(CASE WHEN a.status='completed_paired' THEN a.pair_cost_points END) as avg_cost,
      AVG(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points END) as avg_pair_profit,
      AVG(
        CASE
          WHEN a.pair_profit_points IS NOT NULL THEN a.pair_profit_points
          WHEN a.status = 'completed_failed' THEN -COALESCE(a.stop_loss_threshold_points, a.P1_points)
        END
      ) as avg_profit,
      (SUM(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points ELSE 0 END)
       + SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NOT NULL THEN a.pair_profit_points ELSE 0 END)
       - SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NULL THEN COALESCE(a.stop_loss_threshold_points, a.P1_points) ELSE 0 END))::int as total_pnl,
      COUNT(DISTINCT a.market_id)::int as num_markets
    ${from} ${clause}
  `;

  const rows = await sql.unsafe(query, values as any[]);
  return rows[0] || {};
}

// ---------------------------------------------------------------
// Profitability projection (pure computation, no DB query)
// ---------------------------------------------------------------

export function computeProjection(
  stats: Record<string, any>,
  exitLossPoints = 2,
  numAssets = 4
) {
  const totalAtt = Number(stats.total_attempts) || 0;
  const totalPairs = Number(stats.total_pairs) || 0;
  const avgPairProfit = Number(stats.avg_pair_profit) || 0;
  const numMarkets = Number(stats.num_markets) || 1;

  const R = totalAtt > 0 ? totalPairs / totalAtt : 0;
  const L = exitLossPoints;

  const breakeven = avgPairProfit + L > 0 ? L / (avgPairProfit + L) : 1;
  const evPerAttempt = totalAtt > 0 ? R * avgPairProfit - (1 - R) * L : 0;

  const marketsPerDay = numAssets * 96;
  const avgAttPerMarket = totalAtt / Math.max(1, numMarkets);
  const attemptsPerDay = marketsPerDay * avgAttPerMarket;
  const dailyEv = attemptsPerDay * evPerAttempt;
  const monthlyEv = dailyEv * 30;

  return {
    pair_rate: R,
    avg_profit_points: avgPairProfit,
    exit_loss_points: L,
    breakeven_pair_rate: breakeven,
    ev_per_attempt: evPerAttempt,
    avg_attempts_per_market: avgAttPerMarket,
    markets_per_day: marketsPerDay,
    attempts_per_day: attemptsPerDay,
    daily_ev_points: dailyEv,
    monthly_ev_points: monthlyEv,
    daily_ev_dollars: dailyEv / 100,
    monthly_ev_dollars: monthlyEv / 100,
  };
}

// ---------------------------------------------------------------
// Time series: hourly pattern (aggregated across all days)
// ---------------------------------------------------------------

export async function getTimeSeriesHourly(filters: FilterParams) {
  const sql = getDb();
  const from = baseFrom(filters);
  const { clause, values } = buildWhere(filters);

  const query = `
    SELECT
      EXTRACT(HOUR FROM a.t1_timestamp::timestamp)::int as hour,
      COUNT(*)::int as attempts,
      SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END)::int as pairs,
      AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
      AVG(
        CASE
          WHEN a.pair_profit_points IS NOT NULL THEN a.pair_profit_points
          WHEN a.status = 'completed_failed' THEN -COALESCE(a.stop_loss_threshold_points, a.P1_points)
        END
      ) as avg_profit,
      (SUM(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points ELSE 0 END)
       + SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NOT NULL THEN a.pair_profit_points ELSE 0 END)
       - SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NULL THEN COALESCE(a.stop_loss_threshold_points, a.P1_points) ELSE 0 END))::int as total_pnl
    ${from} ${clause}
    GROUP BY EXTRACT(HOUR FROM a.t1_timestamp::timestamp)
    ORDER BY hour
  `;

  return sql.unsafe(query, values as any[]);
}

// ---------------------------------------------------------------
// Breakdown by a grouping dimension
// ---------------------------------------------------------------

const GROUP_BY_EXPRESSIONS: Record<string, { expr: string; orderBy: string }> =
  {
    delta: {
      expr: "a.delta_points",
      orderBy: "a.delta_points",
    },
    s0: {
      expr: "a.S0_points",
      orderBy: "a.S0_points",
    },
    stopLoss: {
      expr: "a.stop_loss_threshold_points",
      orderBy: "a.stop_loss_threshold_points",
    },
    asset: {
      expr: "m.crypto_asset",
      orderBy: "m.crypto_asset",
    },
    timeRemaining: {
      expr: TIME_REMAINING_CASE,
      orderBy: "MIN(a.time_remaining_at_start) DESC",
    },
    combinedSpread: {
      expr: COMBINED_SPREAD_CASE,
      orderBy:
        "MIN(COALESCE(a.yes_spread_entry_points,0) + COALESCE(a.no_spread_entry_points,0))",
    },
    priceRegime: {
      expr: PRICE_REGIME_CASE,
      orderBy: "MIN(a.reference_yes_points)",
    },
    firstLeg: {
      expr: "a.first_leg_side",
      orderBy: "a.first_leg_side",
    },
    marketPhase: {
      expr: `CASE
        WHEN a.time_remaining_at_start > 600 THEN 'Early (10min+)'
        WHEN a.time_remaining_at_start > 300 THEN 'Middle (5-10min)'
        ELSE 'Late (0-5min)'
      END`,
      orderBy: "MIN(a.time_remaining_at_start) DESC",
    },
    hourOfDay: {
      expr: "EXTRACT(HOUR FROM a.t1_timestamp::timestamp)::int",
      orderBy: "EXTRACT(HOUR FROM a.t1_timestamp::timestamp)::int",
    },
    dayOfWeek: {
      expr: "EXTRACT(DOW FROM a.t1_timestamp::timestamp)::int",
      orderBy: "EXTRACT(DOW FROM a.t1_timestamp::timestamp)::int",
    },
    p1Cost: {
      expr: "a.P1_points",
      orderBy: "a.P1_points",
    },
  };

export type BreakdownGroupBy = keyof typeof GROUP_BY_EXPRESSIONS;

export async function getBreakdown(
  groupBy: BreakdownGroupBy,
  filters: FilterParams
) {
  const sql = getDb();

  // asset groupBy always needs Markets join
  const forceMarketJoin = groupBy === "asset";
  const from = forceMarketJoin
    ? FROM_WITH_MARKETS
    : baseFrom(filters);

  const { clause, values } = buildWhere(filters);
  const group = GROUP_BY_EXPRESSIONS[groupBy];

  if (!group) {
    throw new Error(`Unknown groupBy: ${groupBy}`);
  }

  const query = `
    SELECT
      ${group.expr} as group_key,
      COUNT(*)::int as attempts,
      SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END)::int as pairs,
      SUM(CASE WHEN a.status='completed_failed' THEN 1 ELSE 0 END)::int as failed,
      SUM(CASE WHEN a.fail_reason='stop_loss' THEN 1 ELSE 0 END)::int as stopped,
      AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
      AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp,
      AVG(
        CASE
          WHEN a.pair_profit_points IS NOT NULL THEN a.pair_profit_points
          WHEN a.status = 'completed_failed' THEN -COALESCE(a.stop_loss_threshold_points, a.P1_points)
        END
      ) as avg_profit,
      (SUM(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points ELSE 0 END)
       + SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NOT NULL THEN a.pair_profit_points ELSE 0 END)
       - SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NULL THEN COALESCE(a.stop_loss_threshold_points, a.P1_points) ELSE 0 END))::int as total_pnl,
      AVG(a.max_adverse_excursion_points) as avg_mae
    ${from} ${clause}
    GROUP BY ${group.expr}
    ORDER BY ${group.orderBy}
  `;

  return sql.unsafe(query, values as any[]);
}

// ---------------------------------------------------------------
// Parameter set comparison
// ---------------------------------------------------------------

export async function getParameterComparison() {
  const sql = getDb();

  return sql`
    SELECT
      p.parameter_set_id,
      p.name,
      p.S0_points,
      p.delta_points,
      p.stop_loss_threshold_points,
      COUNT(a.attempt_id)::int as attempts,
      SUM(CASE WHEN a.status='completed_paired' THEN 1 ELSE 0 END)::int as pairs,
      SUM(CASE WHEN a.fail_reason='stop_loss' THEN 1 ELSE 0 END)::int as stopped,
      AVG(CASE WHEN a.status='completed_paired' THEN 1.0 ELSE 0.0 END) as pair_rate,
      AVG(CASE WHEN a.status='completed_paired' THEN a.time_to_pair_seconds END) as avg_ttp,
      AVG(
        CASE
          WHEN a.pair_profit_points IS NOT NULL THEN a.pair_profit_points
          WHEN a.status = 'completed_failed' THEN -COALESCE(a.stop_loss_threshold_points, a.P1_points)
        END
      ) as avg_profit,
      (SUM(CASE WHEN a.status='completed_paired' THEN a.pair_profit_points ELSE 0 END)
       + SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NOT NULL THEN a.pair_profit_points ELSE 0 END)
       - SUM(CASE WHEN a.status != 'completed_paired' AND a.pair_profit_points IS NULL THEN COALESCE(a.stop_loss_threshold_points, a.P1_points) ELSE 0 END))::int as total_pnl
    FROM ParameterSets p
    LEFT JOIN Attempts a ON p.parameter_set_id = a.parameter_set_id
    GROUP BY p.parameter_set_id, p.name, p.S0_points, p.delta_points, p.stop_loss_threshold_points
    ORDER BY p.delta_points ASC, COALESCE(p.stop_loss_threshold_points, 0) ASC
  `;
}
