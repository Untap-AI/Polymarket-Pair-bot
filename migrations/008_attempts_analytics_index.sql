-- 008_attempts_analytics_index.sql
-- Composite partial index for scripts/optimize_params.py.
-- Covers the exact WHERE pattern: status, S0_points, time_remaining_at_start.
-- Also drops redundant single-column indexes subsumed by this one.
--
-- Idempotent: safe to run after migrate_to_partitioned_attempts.py.

BEGIN;

CREATE INDEX IF NOT EXISTS idx_attempts_optimize_params
    ON Attempts (
        delta_points,
        stop_loss_threshold_points,
        P1_points,
        time_remaining_at_start,
        t1_timestamp
    )
    WHERE status IN ('completed_paired', 'completed_failed')
      AND S0_points = 1
      AND time_remaining_at_start <= 900;

DROP INDEX IF EXISTS idx_attempts_status;
DROP INDEX IF EXISTS idx_attempts_delta;

COMMIT;
