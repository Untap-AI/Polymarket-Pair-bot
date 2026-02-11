-- 004_stop_loss_fields.sql
-- Add stop loss support to ParameterSets and Attempts.
--
-- ParameterSets: stop_loss_threshold_points (NULL = no stop loss)
-- Attempts: denormalized threshold + computed stop loss price

BEGIN;

ALTER TABLE ParameterSets
    ADD COLUMN IF NOT EXISTS stop_loss_threshold_points INTEGER;

ALTER TABLE Attempts
    ADD COLUMN IF NOT EXISTS stop_loss_threshold_points INTEGER;

ALTER TABLE Attempts
    ADD COLUMN IF NOT EXISTS stop_loss_price_points INTEGER;

-- Index for filtering by stop loss threshold in analytics
CREATE INDEX IF NOT EXISTS idx_attempts_stop_loss
    ON Attempts(stop_loss_threshold_points);

COMMIT;
