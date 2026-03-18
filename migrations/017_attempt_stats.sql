-- 017_attempt_stats.sql
-- Pre-aggregated live table replacing the grid_4d materialized view.
--
-- Every completed attempt is rolled up into this table on the write path
-- (database.py), so it is always up to date with no manual REFRESH needed.
--
-- Stage 1 of the optimizer and dashboard aggregate endpoints query ~50-150K
-- rows here instead of scanning the 55M-row Attempts table.
--
-- Write path: after each update_attempts_*_batch() call, database.py
-- groups the just-completed rows by (delta, SL, P1, time_minute, asset,
-- date, status, fail_reason, first_leg, hour) and upserts them here.
--
-- Backfill: scripts/backfill_attempt_stats.py populates from existing data.

BEGIN;

CREATE TABLE IF NOT EXISTS attempt_stats (
    delta_points                INT     NOT NULL,
    stop_loss_threshold_points  INT,               -- NULL = no stop loss
    P1_points                   INT     NOT NULL,
    time_minute                 INT     NOT NULL,  -- CEIL(time_remaining_at_start / 60)
    crypto_asset                TEXT    NOT NULL,
    attempt_date                DATE    NOT NULL,
    status                      TEXT    NOT NULL,  -- 'completed_paired' / 'completed_failed'
    fail_reason                 TEXT,              -- NULL, 'stop_loss', 'settlement_reached', etc.
    first_leg_side              TEXT    NOT NULL,  -- 'YES' / 'NO'
    hour_of_day                 INT     NOT NULL,  -- 0-23

    -- Running aggregates
    attempts                    INT     NOT NULL DEFAULT 0,
    pairs                       INT     NOT NULL DEFAULT 0,
    total_pnl                   FLOAT   NOT NULL DEFAULT 0,
    sum_time_to_pair            FLOAT   NOT NULL DEFAULT 0,
    sum_pair_profit             FLOAT   NOT NULL DEFAULT 0
);

-- Unique constraint enabling ON CONFLICT DO UPDATE.
-- COALESCE handles the nullable dimensions so NULL rows form a single group.
-- (NULLS NOT DISTINCT requires PG 15+; COALESCE works on all versions.)
CREATE UNIQUE INDEX IF NOT EXISTS attempt_stats_pk
    ON attempt_stats (
        delta_points,
        COALESCE(stop_loss_threshold_points, -1),
        P1_points,
        time_minute,
        crypto_asset,
        attempt_date,
        status,
        COALESCE(fail_reason, ''),
        first_leg_side,
        hour_of_day
    );

-- Fast filtered queries: date range + market filter (mirrors grid_4d_date_asset)
CREATE INDEX IF NOT EXISTS attempt_stats_date_asset
    ON attempt_stats (attempt_date, crypto_asset);

COMMIT;
