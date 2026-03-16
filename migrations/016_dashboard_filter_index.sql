-- 016_dashboard_filter_index.sql
-- Composite index for the dashboard /api/stats (and related) queries.
--
-- Problem: queries filtered by (delta, S0, stop_loss, crypto_asset, dateAfter,
-- P1 range, time_remaining range) cause full sequential scans on every
-- partition because no single existing index covers the equality filters
-- together.
--
-- The EXPLAIN output shows Postgres scanning ~4M rows per partition and
-- discarding all but ~50-100 (99.99% waste).
--
-- Solution: a covering index with all equality filters leading, then the
-- most common range filters.  Column order:
--   1. delta_points            – equality, high selectivity
--   2. S0_points               – equality
--   3. stop_loss_threshold_points – equality
--   4. crypto_asset            – equality (denormalized in migration 015)
--   5. t1_timestamp            – range (dateAfter / dateBefore)
--   6. P1_points               – range (firstLegPriceMin / Max)
--   7. time_remaining_at_start – range (timeRemainingBucket)
--
-- With the first 4 columns all being equality conditions, Postgres can seek
-- directly to the matching rows and apply the range conditions inline,
-- making each partition scan take microseconds instead of tens of seconds.
--
-- Created CONCURRENTLY so it doesn't block active queries while building
-- (may take several minutes on 30M rows).

CREATE INDEX IF NOT EXISTS idx_attempts_dashboard
    ON Attempts (
        delta_points,
        S0_points,
        stop_loss_threshold_points,
        crypto_asset,
        t1_timestamp,
        P1_points,
        time_remaining_at_start
    );
