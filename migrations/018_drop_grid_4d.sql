-- 018_drop_grid_4d.sql
-- Drop the grid_4d materialized view now that attempt_stats (migration 017)
-- serves the same purpose and is always live (no manual REFRESH needed).
--
-- Run only after:
--   1. Migration 017 has been applied (attempt_stats table exists)
--   2. attempt_stats has been backfilled (scripts/backfill_attempt_stats.py)
--   3. The write path is live (database.py _upsert_attempt_stats deployed)
--   4. optimize_params.py has been updated to query attempt_stats

BEGIN;

DROP MATERIALIZED VIEW IF EXISTS grid_4d CASCADE;

COMMIT;
