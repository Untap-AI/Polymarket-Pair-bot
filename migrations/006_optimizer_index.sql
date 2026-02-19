-- 006_optimizer_index.sql
-- Composite index to speed up the parameter optimizer script and any
-- query that filters by (S0, delta, stop_loss, status) — the exact
-- pattern used by scripts/optimize_params.py.
--
-- Without this index, each optimizer query (56 combos × env breakdown)
-- does a full sequential scan of the Attempts table.  With it, Postgres
-- can seek directly to the rows for each combo in milliseconds.
--
-- Column order matters: put the highest-cardinality equality filters
-- first (S0, delta, stop_loss, status) so the index is maximally
-- selective before touching t1_timestamp for the date grouping.

BEGIN;

CREATE INDEX IF NOT EXISTS idx_attempts_optimizer
    ON Attempts(S0_points, delta_points, stop_loss_threshold_points, status, t1_timestamp);

COMMIT;
