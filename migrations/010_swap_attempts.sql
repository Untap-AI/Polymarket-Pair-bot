-- 010_swap_attempts.sql
-- Swap Attempts and Attempts_part, update part_config, recreate indexes.
-- Idempotent: only runs if Attempts_part exists (i.e. swap not yet done).

BEGIN;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'attempts_part') THEN
    PERFORM setval(
        pg_get_serial_sequence('Attempts_part', 'attempt_id'),
        (SELECT COALESCE(MAX(attempt_id), 0) FROM Attempts_part)
    );

    ALTER TABLE Attempts      RENAME TO Attempts_old;
    ALTER TABLE Attempts_part RENAME TO Attempts;

    UPDATE partman.part_config
    SET parent_table = 'public.Attempts'
    WHERE parent_table = 'public.Attempts_part';

    CREATE INDEX IF NOT EXISTS idx_attempts_market ON Attempts(market_id);
    CREATE INDEX IF NOT EXISTS idx_attempts_param_set ON Attempts(parameter_set_id);
    CREATE INDEX IF NOT EXISTS idx_attempts_optimizer ON Attempts(S0_points, delta_points, stop_loss_threshold_points, status, t1_timestamp);
    CREATE INDEX IF NOT EXISTS idx_attempts_optimize_params
        ON Attempts (delta_points, stop_loss_threshold_points, P1_points, time_remaining_at_start, t1_timestamp)
        WHERE status IN ('completed_paired', 'completed_failed')
          AND S0_points = 1
          AND time_remaining_at_start <= 900;

    DROP TABLE IF EXISTS AttemptLifecycle;
    DROP TABLE IF EXISTS Snapshots;
  END IF;
END $$;

COMMIT;
