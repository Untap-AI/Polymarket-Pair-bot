-- 002_limit_order_fields.sql
-- Add first-leg limit order tracking columns to Attempts table.
-- Supports the maker limit order simulation refactor:
--   - limit_placed_timestamp/cycle: when the first-leg limit was placed/refreshed
--   - cycles_to_fill_first_leg: 0 = same-cycle (highest taker risk), 1+ = delayed
--   - ask_at_placement_points: the ask price when the limit was placed
--   - placement_buffer_points: ask_at_placement - P1 (taker-risk indicator)

BEGIN;

ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS limit_placed_timestamp TEXT;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS limit_placed_cycle INTEGER;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS cycles_to_fill_first_leg INTEGER;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS ask_at_placement_points INTEGER;
ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS placement_buffer_points INTEGER;

-- Index for taker-risk analytics queries
CREATE INDEX IF NOT EXISTS idx_attempts_placement_buffer
    ON Attempts(placement_buffer_points);

CREATE INDEX IF NOT EXISTS idx_attempts_cycles_to_fill
    ON Attempts(cycles_to_fill_first_leg);

COMMIT;


