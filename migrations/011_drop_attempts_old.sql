-- 011_drop_attempts_old.sql
-- Drop Attempts_old after 24-48h verification period.
-- Idempotent: IF EXISTS.

BEGIN;

DROP TABLE IF EXISTS Attempts_old;

COMMIT;
