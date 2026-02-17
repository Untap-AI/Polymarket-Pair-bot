-- 005_performance_indexes.sql
-- Add indexes to optimize dashboard analytics queries.
--
-- Common filter patterns: dateAfter (t1_timestamp), deltaPoints, s0Points.
-- Queries typically filter by date range first, then delta/s0.

BEGIN;

-- Date range is the most common filter; enables index range scan
CREATE INDEX IF NOT EXISTS idx_attempts_t1_timestamp
    ON Attempts(t1_timestamp);

-- S0 filter (when multiple S0 values in use)
CREATE INDEX IF NOT EXISTS idx_attempts_s0
    ON Attempts(S0_points);

-- Composite: date + delta is the default filter combo; very selective
CREATE INDEX IF NOT EXISTS idx_attempts_t1_delta
    ON Attempts(t1_timestamp, delta_points);

COMMIT;
