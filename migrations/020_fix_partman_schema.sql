-- 020_fix_partman_schema.sql
-- Fix pg_partman registration: Supabase installs pg_partman in the `extensions`
-- schema, but migration 009 referenced `partman.*` which left part_config empty
-- and broke automatic partition creation after March 17.
--
-- This was already fixed via direct SQL on 2026-03-27. This migration exists
-- for reproducibility and documents the correct schema references.

BEGIN;

-- Re-register the parent table in the correct schema if not already present.
-- (Idempotent: skips if already registered.)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM extensions.part_config
    WHERE parent_table = 'public.attempts'
  ) THEN
    PERFORM extensions.create_parent(
        p_parent_table    => 'public.attempts',
        p_control         => 'ts',
        p_interval        => '1 day',
        p_premake         => 3,
        p_start_partition => (now() - interval '35 days')::date::text
    );
  END IF;

  -- Ensure retention is set correctly
  UPDATE extensions.part_config
  SET retention = '30 days', retention_keep_table = false
  WHERE parent_table = 'public.attempts';
END $$;

COMMIT;
