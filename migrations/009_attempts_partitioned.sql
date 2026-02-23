-- 009_attempts_partitioned.sql
-- Create Attempts_part table and initialize pg_partman with 30-day retention.
-- Idempotent: safe to run after migrate_to_partitioned_attempts.py.

BEGIN;

CREATE EXTENSION IF NOT EXISTS pg_partman SCHEMA partman;

-- PostgreSQL does not allow generated columns as partition keys.
-- Use a plain TIMESTAMP NOT NULL column populated by a BEFORE INSERT trigger.
CREATE OR REPLACE FUNCTION public.set_attempts_ts()
RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    NEW.ts := NEW.t1_timestamp::TIMESTAMP;
    RETURN NEW;
END;
$$;

CREATE TABLE IF NOT EXISTS public.Attempts_part (
    attempt_id                   SERIAL,
    market_id                    TEXT             NOT NULL REFERENCES Markets(market_id),
    parameter_set_id             INTEGER          NOT NULL REFERENCES ParameterSets(parameter_set_id),
    t1_timestamp                 TEXT             NOT NULL,
    first_leg_side               TEXT             NOT NULL,
    P1_points                    INTEGER          NOT NULL,
    reference_yes_points         INTEGER          NOT NULL,
    status                       TEXT             NOT NULL DEFAULT 'active',
    t2_timestamp                 TEXT,
    time_to_pair_seconds         DOUBLE PRECISION,
    time_remaining_at_start      DOUBLE PRECISION,
    time_remaining_at_completion DOUBLE PRECISION,
    actual_opposite_price        INTEGER,
    pair_cost_points             INTEGER,
    pair_profit_points           INTEGER,
    fail_reason                  TEXT,
    had_feed_gap                 INTEGER          DEFAULT 0,
    closest_approach_points      INTEGER,
    max_adverse_excursion_points INTEGER,
    yes_spread_entry_points      INTEGER,
    no_spread_entry_points       INTEGER,
    yes_spread_exit_points       INTEGER,
    no_spread_exit_points        INTEGER,
    delta_points                 INTEGER,
    S0_points                    INTEGER,
    stop_loss_threshold_points   INTEGER,
    ts                           TIMESTAMP        NOT NULL,
    PRIMARY KEY (attempt_id, ts)
) PARTITION BY RANGE (ts);

CREATE OR REPLACE TRIGGER set_ts_before_insert
    BEFORE INSERT ON public.Attempts_part
    FOR EACH ROW EXECUTE FUNCTION public.set_attempts_ts();

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM partman.part_config WHERE parent_table = 'public.Attempts_part') THEN
    PERFORM partman.create_parent(
        p_parent_table    => 'public.Attempts_part',
        p_control         => 'ts',
        p_interval        => '1 day',
        p_premake         => 3,
        p_start_partition => (now() - interval '35 days')::date::text
    );
    UPDATE partman.part_config
    SET retention = '30 days', retention_keep_table = false
    WHERE parent_table = 'public.Attempts_part';
  END IF;
END $$;

COMMIT;
