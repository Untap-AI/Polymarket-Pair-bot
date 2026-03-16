## Fix default partition — Run each step in order

### Step 1: Detach the default partition

```sql
ALTER TABLE public.attempts
    DETACH PARTITION public.attempts_part_default;
```

### Step 2: Create the missing partitions

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
    END LOOP;
END $$;
```

### Step 3: Move rows from default into the correct partitions (one day at a time)

Run each of these individually. They may take a minute each given the volume.

```sql
INSERT INTO public.attempts
SELECT * FROM public.attempts_part_default
WHERE ts >= '2026-03-09' AND ts < '2026-03-10';

DELETE FROM public.attempts_part_default
WHERE ts >= '2026-03-09' AND ts < '2026-03-10';
```

```sql
INSERT INTO public.attempts
SELECT * FROM public.attempts_part_default
WHERE ts >= '2026-03-10' AND ts < '2026-03-11';

DELETE FROM public.attempts_part_default
WHERE ts >= '2026-03-10' AND ts < '2026-03-11';
```

```sql
INSERT INTO public.attempts
SELECT * FROM public.attempts_part_default
WHERE ts >= '2026-03-11' AND ts < '2026-03-12';

DELETE FROM public.attempts_part_default
WHERE ts >= '2026-03-11' AND ts < '2026-03-12';
```

```sql
INSERT INTO public.attempts
SELECT * FROM public.attempts_part_default
WHERE ts >= '2026-03-12' AND ts < '2026-03-13';

DELETE FROM public.attempts_part_default
WHERE ts >= '2026-03-12' AND ts < '2026-03-13';
```

```sql
INSERT INTO public.attempts
SELECT * FROM public.attempts_part_default
WHERE ts >= '2026-03-13' AND ts < '2026-03-14';

DELETE FROM public.attempts_part_default
WHERE ts >= '2026-03-13' AND ts < '2026-03-14';
```

```sql
INSERT INTO public.attempts
SELECT * FROM public.attempts_part_default
WHERE ts >= '2026-03-14' AND ts < '2026-03-15';

DELETE FROM public.attempts_part_default
WHERE ts >= '2026-03-14' AND ts < '2026-03-15';
```

### Step 4: Verify default is empty

```sql
SELECT COUNT(*) as remaining FROM public.attempts_part_default;
```

### Step 5: Re-attach the default partition

```sql
ALTER TABLE public.attempts
    ATTACH PARTITION public.attempts_part_default DEFAULT;
```

### Step 6: Verify partitions look right

```sql
SELECT tablename
FROM pg_tables
WHERE tablename LIKE 'attempts_part%'
ORDER BY tablename;
```

### Step 7: Now continue with the rest of fix_partitions.md

Go back to `fix_partitions.md` and run Steps 4-8 (drop old partitions,
fix pg_partman config, schedule correct cron jobs).
