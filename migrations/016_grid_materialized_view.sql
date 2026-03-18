-- 016_grid_materialized_view.sql
-- Pre-aggregate the 4D optimizer grid so Stage 1 runs in seconds instead of ~2 hours.
--
-- Problem: Stage 1 in scripts/optimize_params.py does a GROUP BY aggregation
-- over all ~30M rows in Attempts.  No index can avoid this — COUNT(*)/SUM()
-- must visit every qualifying row.  The idx_attempts_stage3 index (migration 013)
-- is designed for Stage 3's per-config seeks and does not help here.
--
-- Solution: materialise the aggregation keyed by
--   (delta, SL, P1, time_minute, crypto_asset, attempt_date)
-- Refreshing the view scans the full table once (a few minutes), then Stage 1
-- queries a few thousand rows and completes in milliseconds.
--
-- Refresh (run before each optimizer invocation, or via pg_cron daily):
--   REFRESH MATERIALIZED VIEW CONCURRENTLY grid_4d;
--
-- Or via pg_cron:
--   SELECT cron.schedule('refresh-grid-4d', '0 3 * * *',
--       'REFRESH MATERIALIZED VIEW CONCURRENTLY grid_4d');
--
-- Net-PNL formula (mirrors NET_PNL_EXPR in optimize_params.py):
--   completed_paired          → +delta_points
--   completed_failed, SL hit  → -(SL + taker_fee)   taker_fee = 100*0.25*(p*(1-p))^2
--   completed_failed, no SL   → -P1_points
-- If the fee constants change, drop and recreate this view.

BEGIN;

CREATE MATERIALIZED VIEW IF NOT EXISTS grid_4d AS
SELECT
    delta_points,
    stop_loss_threshold_points,
    P1_points,
    CEIL(time_remaining_at_start / 60)::int          AS time_minute,
    crypto_asset,
    DATE(t1_timestamp::timestamp)                    AS attempt_date,
    COUNT(*)                                         AS attempts,
    SUM(CASE WHEN status = 'completed_paired' THEN 1 ELSE 0 END) AS pairs,
    SUM(
        CASE
            WHEN status = 'completed_paired' THEN delta_points
            WHEN status = 'completed_failed'
                 AND stop_loss_threshold_points IS NOT NULL
                 AND P1_points >= stop_loss_threshold_points
                THEN -(stop_loss_threshold_points + (
                    100.0 * 0.25 * POWER(
                        ((P1_points - stop_loss_threshold_points) / 100.0)
                        * (1.0 - (P1_points - stop_loss_threshold_points) / 100.0),
                        2
                    )
                ))
            WHEN status = 'completed_failed' THEN -P1_points
            ELSE 0
        END
    )::float                                         AS total_pnl,
    MIN(t1_timestamp::timestamp)                     AS min_ts,
    MAX(t1_timestamp::timestamp)                     AS max_ts
FROM Attempts
WHERE status IN ('completed_paired', 'completed_failed')
  AND S0_points = 1
GROUP BY 1, 2, 3, 4, 5, 6;

-- Supports REFRESH MATERIALIZED VIEW CONCURRENTLY.
-- COALESCE on nullable columns so NULL rows are treated as a single group
-- (works on all PostgreSQL versions; NULLS NOT DISTINCT requires PG 15+).
CREATE UNIQUE INDEX IF NOT EXISTS grid_4d_pk
    ON grid_4d (
        delta_points,
        COALESCE(stop_loss_threshold_points, -1),
        P1_points,
        time_minute,
        COALESCE(crypto_asset, ''),
        attempt_date
    );

-- Fast filtered queries: date range + market filter used by Stage 1.
CREATE INDEX IF NOT EXISTS grid_4d_date_asset
    ON grid_4d (attempt_date, crypto_asset);

COMMIT;
