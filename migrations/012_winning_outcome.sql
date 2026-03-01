-- 012_winning_outcome.sql
-- Add winning_outcome column to Markets to record which side won ('yes' or 'no').
-- NULL means the outcome has not been recorded yet (older rows or unresolved markets).

BEGIN;

ALTER TABLE Markets ADD COLUMN IF NOT EXISTS winning_outcome TEXT;

COMMIT;
