## Fix Partitions — Run in Supabase SQL Editor (in order)

### Step 1: Confirm the parent table name

```sql
SELECT inhparent::regclass AS parent, inhrelid::regclass AS child
FROM pg_inherits
WHERE inhparent::regclass::text ILIKE '%attempt%'
LIMIT 5;
```

### Step 2: Delete the broken cron jobs

```sql
SELECT cron.unschedule(2);
SELECT cron.unschedule(3);
```

### Step 3: Create missing partitions (March 9 → March 17)

```sql
DO $$
DECLARE
    d date;
    tname text;
BEGIN
    FOR d IN SELECT generate_series(
        '2026-03-09'::date,
        '2026-03-17'::date,
        '1 day'::interval
    )::date LOOP
        tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.attempts FOR VALUES FROM (%L) TO (%L)',
            tname, d::timestamp, (d + 1)::timestamp
        );
        RAISE NOTICE 'Created %', tname;
    END LOOP;
END $$;
```

### Step 4: Drop old partitions (older than 30 days)

```sql
DO $$
DECLARE
    d date;
    tname text;
BEGIN
    FOR d IN SELECT generate_series(
        '2026-02-04'::date,
        (CURRENT_DATE - 30)::date,
        '1 day'::interval
    )::date LOOP
        tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
        EXECUTE format('DROP TABLE IF EXISTS public.%I', tname);
        RAISE NOTICE 'Dropped %', tname;
    END LOOP;
END $$;
```

### Step 5: Check if pg_partman knows about the table

```sql
SELECT parent_table, control, partition_interval, premake,
       retention, retention_keep_table
FROM extensions.part_config;
```

If empty or pointing to wrong table, re-register:

```sql
-- Remove old config if present
DELETE FROM extensions.part_config
WHERE parent_table IN ('public.attempts_part', 'public.attempts');

-- Register with correct table name
SELECT extensions.create_parent(
    p_parent_table    => 'public.attempts',
    p_control         => 'ts',
    p_interval        => '1 day',
    p_premake         => 3
);

UPDATE extensions.part_config
SET retention = '30 days', retention_keep_table = false
WHERE parent_table = 'public.attempts';
```

### Step 6: Verify pg_partman maintenance works

```sql
CALL extensions.run_maintenance_proc();
```

Then check:

```sql
SELECT tablename
FROM pg_tables
WHERE tablename LIKE 'attempts_part%'
ORDER BY tablename;
```

If pg_partman is creating/pruning correctly, you're done — Job 1
already calls `extensions.run_maintenance_proc()` hourly.

### Step 7: Manual cron fallback (only if Step 5/6 failed)

```sql
SELECT cron.schedule(
    'create-partitions',
    '0 23 * * *',
    $cron$
    DO $inner$
    DECLARE
        d date;
        tname text;
    BEGIN
        FOR d IN SELECT generate_series(
            (CURRENT_DATE + 1)::timestamp,
            (CURRENT_DATE + 3)::timestamp,
            '1 day'::interval
        )::date LOOP
            tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS public.%I PARTITION OF public.attempts FOR VALUES FROM (%L) TO (%L)',
                tname, d::timestamp, (d + 1)::timestamp
            );
        END LOOP;
    END $inner$
    $cron$
);

SELECT cron.schedule(
    'drop-old-partitions',
    '0 23 * * *',
    $cron$
    DO $inner$
    DECLARE
        d date;
        tname text;
    BEGIN
        FOR d IN SELECT generate_series(
            (CURRENT_DATE - 35)::date,
            (CURRENT_DATE - 30)::date,
            '1 day'::interval
        )::date LOOP
            tname := 'attempts_part_p' || to_char(d, 'YYYYMMDD');
            EXECUTE format('DROP TABLE IF EXISTS public.%I', tname);
        END LOOP;
    END $inner$
    $cron$
);
```

### Step 8: Verify

```sql
SELECT jobid, schedule, command, active FROM cron.job;

SELECT jobid, status, return_message, start_time
FROM cron.job_run_details
ORDER BY start_time DESC
LIMIT 10;

SELECT tablename FROM pg_tables
WHERE tablename LIKE 'attempts_part%'
ORDER BY tablename;
```
