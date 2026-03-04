-- 013_stage3_market_index.sql
-- Covering index for Stage 3 queries in scripts/optimize_params.py.
--
-- Stage 3 fires one query per config:
--
--   SELECT DISTINCT ON (market_id) …
--   FROM Attempts
--   WHERE status IN ('completed_paired','completed_failed')
--     AND S0_points = 1
--     AND time_remaining_at_start <= 900
--     AND delta_points = $X
--     AND stop_loss_threshold_points = $Y
--     AND P1_points BETWEEN $lo AND $hi
--     AND CEIL(time_remaining_at_start / 60)::int BETWEEN $ta AND $tb
--   ORDER BY market_id, t1_timestamp ASC
--
-- The existing idx_attempts_optimize_params covers the WHERE filter
-- columns but NOT market_id, so Postgres has to sort the filtered rows
-- by market_id for DISTINCT ON — an extra sort step on every query.
--
-- Adding market_id before t1_timestamp lets Postgres resolve the
-- DISTINCT ON directly from the index scan without a sort.
--
-- The old idx_attempts_optimize_params index is replaced by the new
-- one (same partial conditions, same leading columns, plus market_id).

BEGIN;

CREATE INDEX IF NOT EXISTS idx_attempts_stage3
    ON Attempts (
        delta_points,
        stop_loss_threshold_points,
        P1_points,
        time_remaining_at_start,
        market_id,
        t1_timestamp
    )
    WHERE status IN ('completed_paired', 'completed_failed')
      AND S0_points = 1
      AND time_remaining_at_start <= 900;

-- The new index strictly supersedes the old one for all optimizer queries.
DROP INDEX IF EXISTS idx_attempts_optimize_params;

COMMIT;
