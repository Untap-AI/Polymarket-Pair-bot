-- 007_drop_unused_columns.sql
-- Remove Attempts columns that are never queried in analytics,
-- the dashboard, or the optimizer script.
--
-- Columns retained for runtime use (read by TriggerEvaluator in memory
-- but not needed in the DB):
--   opposite_side, opposite_trigger_points — drive pairing checks each cycle
--   stop_loss_price_points — drives stop loss checks each cycle
--   These three are kept only in the in-memory Attempt model; they are no
--   longer persisted starting with this migration.
--
-- Columns retained despite being unqueried (conservative — future value):
--   t2_timestamp, time_remaining_at_completion, actual_opposite_price,
--   had_feed_gap
--
-- Columns removed: cycle_number, reference_no_points, opposite_side,
--   opposite_trigger_points, opposite_max_points, t2_cycle_number,
--   time_remaining_bucket, closest_approach_timestamp,
--   closest_approach_cycle_number, mae_timestamp, mae_cycle_number,
--   stop_loss_price_points

BEGIN;

ALTER TABLE Attempts DROP COLUMN IF EXISTS cycle_number;
ALTER TABLE Attempts DROP COLUMN IF EXISTS reference_no_points;
ALTER TABLE Attempts DROP COLUMN IF EXISTS opposite_side;
ALTER TABLE Attempts DROP COLUMN IF EXISTS opposite_trigger_points;
ALTER TABLE Attempts DROP COLUMN IF EXISTS opposite_max_points;
ALTER TABLE Attempts DROP COLUMN IF EXISTS t2_cycle_number;
ALTER TABLE Attempts DROP COLUMN IF EXISTS time_remaining_bucket;
ALTER TABLE Attempts DROP COLUMN IF EXISTS closest_approach_timestamp;
ALTER TABLE Attempts DROP COLUMN IF EXISTS closest_approach_cycle_number;
ALTER TABLE Attempts DROP COLUMN IF EXISTS mae_timestamp;
ALTER TABLE Attempts DROP COLUMN IF EXISTS mae_cycle_number;
ALTER TABLE Attempts DROP COLUMN IF EXISTS stop_loss_price_points;

COMMIT;
