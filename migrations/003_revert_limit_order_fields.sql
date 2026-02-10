-- 003_revert_limit_order_fields.sql
-- Revert 002_limit_order_fields.sql: remove first-leg limit order tracking.
-- Use this migration to roll back the maker limit order simulation changes
-- if reverting that feature in the upstream repo.

BEGIN;

-- Drop indexes first
DROP INDEX IF EXISTS idx_attempts_placement_buffer;
DROP INDEX IF EXISTS idx_attempts_cycles_to_fill;

-- Drop columns
ALTER TABLE Attempts DROP COLUMN IF EXISTS limit_placed_timestamp;
ALTER TABLE Attempts DROP COLUMN IF EXISTS limit_placed_cycle;
ALTER TABLE Attempts DROP COLUMN IF EXISTS cycles_to_fill_first_leg;
ALTER TABLE Attempts DROP COLUMN IF EXISTS ask_at_placement_points;
ALTER TABLE Attempts DROP COLUMN IF EXISTS placement_buffer_points;

COMMIT;
