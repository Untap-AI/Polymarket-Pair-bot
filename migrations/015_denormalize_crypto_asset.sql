-- 015_denormalize_crypto_asset.sql
-- Denormalize crypto_asset from Markets into Attempts, following the same
-- pattern as delta_points / S0_points from ParameterSets.
--
-- This eliminates the need to JOIN Markets when the asset filter is used
-- in dashboard queries, allowing full index-only scans on Attempts.
--
-- Step 1 of 2: schema change only (fast, no row locking).
-- Backfill the existing ~30M rows separately using:
--   python scripts/backfill_crypto_asset.py
-- The index is created after backfill completes (see that script).

BEGIN;

ALTER TABLE Attempts ADD COLUMN IF NOT EXISTS crypto_asset TEXT;

COMMIT;
