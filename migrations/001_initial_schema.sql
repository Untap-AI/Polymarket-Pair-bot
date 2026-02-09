-- 001_initial_schema.sql
-- Full initial schema for Polymarket Pair Measurement Bot (PostgreSQL)
--
-- Tables: ParameterSets, Markets, Attempts, Snapshots, AttemptLifecycle
-- Note: Attempts includes denormalized delta_points / S0_points for
--       easier analytics without JOINs.

BEGIN;

-- ----------------------------------------------------------------
-- ParameterSets
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ParameterSets (
    parameter_set_id            SERIAL PRIMARY KEY,
    name                        TEXT    NOT NULL,
    S0_points                   INTEGER NOT NULL,
    delta_points                INTEGER NOT NULL,
    PairCap_points              INTEGER NOT NULL,
    trigger_rule                TEXT    NOT NULL,
    reference_price_source      TEXT    NOT NULL,
    tie_break_rule              TEXT    DEFAULT 'distance_then_yes',
    sampling_mode               TEXT,
    cycle_interval_seconds      DOUBLE PRECISION,
    cycles_per_market           INTEGER,
    feed_gap_threshold_seconds  DOUBLE PRECISION,
    created_at                  TEXT    NOT NULL
);

-- ----------------------------------------------------------------
-- Markets
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Markets (
    market_id                   TEXT PRIMARY KEY,
    crypto_asset                TEXT    NOT NULL,
    condition_id                TEXT    NOT NULL,
    yes_token_id                TEXT    NOT NULL,
    no_token_id                 TEXT    NOT NULL,
    start_time                  TEXT    NOT NULL,
    settlement_time             TEXT    NOT NULL,
    actual_settlement_time      TEXT,
    tick_size_points            INTEGER NOT NULL,
    parameter_set_id            INTEGER REFERENCES ParameterSets(parameter_set_id),
    total_attempts              INTEGER DEFAULT 0,
    total_pairs                 INTEGER DEFAULT 0,
    total_failed                INTEGER DEFAULT 0,
    settlement_failures         INTEGER DEFAULT 0,
    pair_rate                   DOUBLE PRECISION,
    avg_time_to_pair            DOUBLE PRECISION,
    median_time_to_pair         DOUBLE PRECISION,
    max_concurrent_attempts     INTEGER DEFAULT 0,
    total_cycles_run            INTEGER DEFAULT 0,
    cycle_interval_seconds      DOUBLE PRECISION,
    time_remaining_at_start     DOUBLE PRECISION,
    anomaly_count               INTEGER DEFAULT 0,
    notes                       TEXT
);

-- ----------------------------------------------------------------
-- Attempts
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Attempts (
    attempt_id                      SERIAL PRIMARY KEY,
    market_id                       TEXT    NOT NULL REFERENCES Markets(market_id),
    parameter_set_id                INTEGER NOT NULL REFERENCES ParameterSets(parameter_set_id),
    cycle_number                    INTEGER NOT NULL,
    t1_timestamp                    TEXT    NOT NULL,
    first_leg_side                  TEXT    NOT NULL,
    P1_points                       INTEGER NOT NULL,
    reference_yes_points            INTEGER NOT NULL,
    reference_no_points             INTEGER NOT NULL,
    opposite_side                   TEXT    NOT NULL,
    opposite_trigger_points         INTEGER NOT NULL,
    opposite_max_points             INTEGER NOT NULL,
    status                          TEXT    NOT NULL DEFAULT 'active',
    t2_timestamp                    TEXT,
    t2_cycle_number                 INTEGER,
    time_to_pair_seconds            DOUBLE PRECISION,
    time_remaining_at_start         DOUBLE PRECISION,
    time_remaining_at_completion    DOUBLE PRECISION,
    actual_opposite_price           INTEGER,
    pair_cost_points                INTEGER,
    pair_profit_points              INTEGER,
    fail_reason                     TEXT,
    had_feed_gap                    INTEGER DEFAULT 0,
    closest_approach_points         INTEGER,
    closest_approach_timestamp      TEXT,
    closest_approach_cycle_number   INTEGER,
    max_adverse_excursion_points    INTEGER,
    mae_timestamp                   TEXT,
    mae_cycle_number                INTEGER,
    time_remaining_bucket           TEXT,
    yes_spread_entry_points         INTEGER,
    no_spread_entry_points          INTEGER,
    yes_spread_exit_points          INTEGER,
    no_spread_exit_points           INTEGER,
    -- Denormalized from ParameterSets for easier analytics
    delta_points                    INTEGER,
    S0_points                       INTEGER
);

-- ----------------------------------------------------------------
-- Snapshots (optional — enable_snapshots)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS Snapshots (
    snapshot_id                 SERIAL PRIMARY KEY,
    market_id                   TEXT    NOT NULL REFERENCES Markets(market_id),
    cycle_number                INTEGER NOT NULL,
    timestamp                   TEXT    NOT NULL,
    yes_bid_points              INTEGER,
    yes_ask_points              INTEGER,
    no_bid_points               INTEGER,
    no_ask_points               INTEGER,
    yes_last_trade_points       INTEGER,
    no_last_trade_points        INTEGER,
    time_remaining              DOUBLE PRECISION,
    active_attempts_count       INTEGER DEFAULT 0,
    anomaly_flag                INTEGER DEFAULT 0,
    yes_period_low_ask_points   INTEGER,
    no_period_low_ask_points    INTEGER
);

-- ----------------------------------------------------------------
-- AttemptLifecycle (optional — enable_lifecycle_tracking)
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS AttemptLifecycle (
    lifecycle_id                SERIAL PRIMARY KEY,
    attempt_id                  INTEGER NOT NULL REFERENCES Attempts(attempt_id),
    cycle_number                INTEGER NOT NULL,
    timestamp                   TEXT    NOT NULL,
    opposite_ask_points         INTEGER,
    distance_to_trigger         INTEGER,
    closest_approach_so_far     INTEGER
);

-- ----------------------------------------------------------------
-- Indexes for common query patterns
-- ----------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_attempts_market
    ON Attempts(market_id);

CREATE INDEX IF NOT EXISTS idx_attempts_param_set
    ON Attempts(parameter_set_id);

CREATE INDEX IF NOT EXISTS idx_attempts_status
    ON Attempts(status);

CREATE INDEX IF NOT EXISTS idx_attempts_delta
    ON Attempts(delta_points);

CREATE INDEX IF NOT EXISTS idx_snapshots_market
    ON Snapshots(market_id);

CREATE INDEX IF NOT EXISTS idx_lifecycle_attempt
    ON AttemptLifecycle(attempt_id);

CREATE INDEX IF NOT EXISTS idx_markets_asset
    ON Markets(crypto_asset);

COMMIT;

