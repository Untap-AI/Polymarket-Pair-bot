/**
 * Shared filter types and SQL WHERE clause builder.
 *
 * Every API route and query function uses FilterParams as
 * the single source of truth for what data subset to query.
 */

export interface FilterParams {
  deltaPoints?: number[];
  s0Points?: number[];
  stopLoss?: (number | null)[];
  hourRange?: [number, number]; // 0-23
  daysOfWeek?: number[]; // 0=Sun..6=Sat
  timeRemainingBucket?: string[];
  combinedSpreadBucket?: string[];
  priceRegime?: string[];
  dateAfter?: string;
  dateBefore?: string;
  asset?: string[];
  parameterSetId?: number;
  firstLegSide?: string[];
  status?: string[];
}

/**
 * Parse FilterParams from a URLSearchParams object.
 * Used by API route handlers.
 */
export function parseFiltersFromParams(
  params: URLSearchParams
): FilterParams {
  const filters: FilterParams = {};

  const delta = params.get("deltaPoints");
  if (delta) filters.deltaPoints = delta.split(",").map(Number);

  const s0 = params.get("s0Points");
  if (s0) filters.s0Points = s0.split(",").map(Number);

  const sl = params.get("stopLoss");
  if (sl)
    filters.stopLoss = sl.split(",").map((v) => (v === "null" ? null : Number(v)));

  const hourMin = params.get("hourMin");
  const hourMax = params.get("hourMax");
  if (hourMin !== null && hourMax !== null && hourMin !== "" && hourMax !== "") {
    filters.hourRange = [Number(hourMin), Number(hourMax)];
  }

  const dow = params.get("daysOfWeek");
  if (dow) filters.daysOfWeek = dow.split(",").map(Number);

  const trb = params.get("timeRemainingBucket");
  if (trb) filters.timeRemainingBucket = trb.split(",");

  const csb = params.get("combinedSpreadBucket");
  if (csb) filters.combinedSpreadBucket = csb.split(",");

  const regime = params.get("priceRegime");
  if (regime) filters.priceRegime = regime.split(",");

  const after = params.get("dateAfter");
  if (after) filters.dateAfter = after;

  const before = params.get("dateBefore");
  if (before) filters.dateBefore = before;

  const asset = params.get("asset");
  if (asset) filters.asset = asset.split(",");

  const psId = params.get("parameterSetId");
  if (psId) filters.parameterSetId = Number(psId);

  const fls = params.get("firstLegSide");
  if (fls) filters.firstLegSide = fls.split(",");

  const status = params.get("status");
  if (status) filters.status = status.split(",");

  return filters;
}

/**
 * Serialize FilterParams into URLSearchParams for fetch calls.
 */
export function filtersToSearchParams(filters: FilterParams): string {
  const p = new URLSearchParams();

  if (filters.deltaPoints?.length)
    p.set("deltaPoints", filters.deltaPoints.join(","));
  if (filters.s0Points?.length)
    p.set("s0Points", filters.s0Points.join(","));
  if (filters.stopLoss?.length)
    p.set(
      "stopLoss",
      filters.stopLoss.map((v) => (v === null ? "null" : String(v))).join(",")
    );
  if (filters.hourRange) {
    p.set("hourMin", String(filters.hourRange[0]));
    p.set("hourMax", String(filters.hourRange[1]));
  }
  if (filters.daysOfWeek?.length)
    p.set("daysOfWeek", filters.daysOfWeek.join(","));
  if (filters.timeRemainingBucket?.length)
    p.set("timeRemainingBucket", filters.timeRemainingBucket.join(","));
  if (filters.combinedSpreadBucket?.length)
    p.set("combinedSpreadBucket", filters.combinedSpreadBucket.join(","));
  if (filters.priceRegime?.length)
    p.set("priceRegime", filters.priceRegime.join(","));
  if (filters.dateAfter) p.set("dateAfter", filters.dateAfter);
  if (filters.dateBefore) p.set("dateBefore", filters.dateBefore);
  if (filters.asset?.length) p.set("asset", filters.asset.join(","));
  if (filters.parameterSetId)
    p.set("parameterSetId", String(filters.parameterSetId));
  if (filters.firstLegSide?.length)
    p.set("firstLegSide", filters.firstLegSide.join(","));
  if (filters.status?.length) p.set("status", filters.status.join(","));

  return p.toString();
}

// ---------------------------------------------------------------
// Price regime CASE expression (mirrors metrics.py)
// ---------------------------------------------------------------
export const PRICE_REGIME_CASE = `
  CASE
    WHEN a.reference_yes_points BETWEEN 45 AND 55 THEN 'Balanced (45-55)'
    WHEN a.reference_yes_points BETWEEN 56 AND 70 THEN 'YES-favored (56-70)'
    WHEN a.reference_yes_points BETWEEN 30 AND 44 THEN 'NO-favored (30-44)'
    ELSE 'Extreme (<30 or >70)'
  END
`;

// Time remaining bucket CASE (1-minute granularity)
export const TIME_REMAINING_CASE = `
  CASE
    WHEN a.time_remaining_at_start >= 840 THEN '15 min'
    WHEN a.time_remaining_at_start >= 780 THEN '14 min'
    WHEN a.time_remaining_at_start >= 720 THEN '13 min'
    WHEN a.time_remaining_at_start >= 660 THEN '12 min'
    WHEN a.time_remaining_at_start >= 600 THEN '11 min'
    WHEN a.time_remaining_at_start >= 540 THEN '10 min'
    WHEN a.time_remaining_at_start >= 480 THEN '9 min'
    WHEN a.time_remaining_at_start >= 420 THEN '8 min'
    WHEN a.time_remaining_at_start >= 360 THEN '7 min'
    WHEN a.time_remaining_at_start >= 300 THEN '6 min'
    WHEN a.time_remaining_at_start >= 240 THEN '5 min'
    WHEN a.time_remaining_at_start >= 180 THEN '4 min'
    WHEN a.time_remaining_at_start >= 120 THEN '3 min'
    WHEN a.time_remaining_at_start >= 60  THEN '2 min'
    WHEN a.time_remaining_at_start >= 0   THEN '1 min'
    ELSE '0 min'
  END
`;

// Combined spread bucket CASE
export const COMBINED_SPREAD_CASE = `
  CASE
    WHEN (a.yes_spread_entry_points + a.no_spread_entry_points) <= 2 THEN 'Tight (<=2)'
    WHEN (a.yes_spread_entry_points + a.no_spread_entry_points) <= 4 THEN 'Normal (3-4)'
    WHEN (a.yes_spread_entry_points + a.no_spread_entry_points) <= 6 THEN 'Wide (5-6)'
    ELSE 'Very wide (7+)'
  END
`;

// ---------------------------------------------------------------
// WHERE clause builder
// ---------------------------------------------------------------

interface WhereResult {
  clause: string;
  values: unknown[];
}

/**
 * Build a WHERE clause + positional params ($1, $2, …) from filters.
 * Always references the Attempts table aliased as "a" and optionally
 * Markets aliased as "m".
 */
export function buildWhere(
  filters: FilterParams,
  /** Starting parameter index (default 1) */
  startIdx = 1
): WhereResult {
  const clauses: string[] = [];
  const values: unknown[] = [];
  let idx = startIdx;

  if (filters.deltaPoints?.length) {
    clauses.push(`a.delta_points = ANY($${idx++})`);
    values.push(filters.deltaPoints);
  }

  if (filters.s0Points?.length) {
    clauses.push(`a.S0_points = ANY($${idx++})`);
    values.push(filters.s0Points);
  }

  if (filters.stopLoss?.length) {
    const nonNull = filters.stopLoss.filter((v) => v !== null) as number[];
    const hasNull = filters.stopLoss.includes(null);
    if (nonNull.length && hasNull) {
      clauses.push(
        `(a.stop_loss_threshold_points = ANY($${idx++}) OR a.stop_loss_threshold_points IS NULL)`
      );
      values.push(nonNull);
    } else if (nonNull.length) {
      clauses.push(`a.stop_loss_threshold_points = ANY($${idx++})`);
      values.push(nonNull);
    } else if (hasNull) {
      clauses.push(`a.stop_loss_threshold_points IS NULL`);
    }
  }

  if (filters.hourRange) {
    const [minH, maxH] = filters.hourRange;
    if (minH <= maxH) {
      clauses.push(
        `EXTRACT(HOUR FROM a.t1_timestamp::timestamp) BETWEEN $${idx++} AND $${idx++}`
      );
      values.push(minH, maxH);
    } else {
      // Wraps around midnight, e.g. 22-4
      clauses.push(
        `(EXTRACT(HOUR FROM a.t1_timestamp::timestamp) >= $${idx++} OR EXTRACT(HOUR FROM a.t1_timestamp::timestamp) <= $${idx++})`
      );
      values.push(minH, maxH);
    }
  }

  if (filters.daysOfWeek?.length) {
    clauses.push(
      `EXTRACT(DOW FROM a.t1_timestamp::timestamp) = ANY($${idx++})`
    );
    values.push(filters.daysOfWeek);
  }

  if (filters.timeRemainingBucket?.length) {
    // Filter by matching the time remaining CASE expression
    clauses.push(`${TIME_REMAINING_CASE} = ANY($${idx++})`);
    values.push(filters.timeRemainingBucket);
  }

  if (filters.combinedSpreadBucket?.length) {
    clauses.push(`${COMBINED_SPREAD_CASE} = ANY($${idx++})`);
    values.push(filters.combinedSpreadBucket);
  }

  if (filters.priceRegime?.length) {
    clauses.push(`${PRICE_REGIME_CASE} = ANY($${idx++})`);
    values.push(filters.priceRegime);
  }

  if (filters.dateAfter) {
    clauses.push(`a.t1_timestamp >= $${idx++}`);
    values.push(filters.dateAfter);
  }

  if (filters.dateBefore) {
    clauses.push(`a.t1_timestamp <= $${idx++}`);
    values.push(filters.dateBefore);
  }

  if (filters.asset?.length) {
    clauses.push(`m.crypto_asset = ANY($${idx++})`);
    values.push(filters.asset.map((a) => a.toLowerCase()));
  }

  if (filters.parameterSetId) {
    clauses.push(`a.parameter_set_id = $${idx++}`);
    values.push(filters.parameterSetId);
  }

  if (filters.firstLegSide?.length) {
    clauses.push(`a.first_leg_side = ANY($${idx++})`);
    values.push(filters.firstLegSide);
  }

  if (filters.status?.length) {
    clauses.push(`a.status = ANY($${idx++})`);
    values.push(filters.status);
  }

  const clause = clauses.length ? "WHERE " + clauses.join(" AND ") : "";
  return { clause, values };
}

/**
 * Whether the filters reference the Markets table (need a JOIN).
 */
export function needsMarketJoin(filters: FilterParams): boolean {
  return !!(filters.asset?.length);
}
