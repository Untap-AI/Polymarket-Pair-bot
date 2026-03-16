## Partition Diagnostics — Run in Supabase SQL Editor

### 1. Is pg_cron enabled and is the job scheduled?

```sql
SELECT jobid, schedule, command, nodename, active
FROM cron.job;
```

### 2. Has the cron job actually run? Any errors?

```sql
SELECT jobid, status, return_message, start_time, end_time
FROM cron.job_run_details
ORDER BY start_time DESC
LIMIT 20;
```

### 3. What does pg_partman's config look like?

```sql
SELECT parent_table, control, partition_interval, premake,
       retention, retention_keep_table, datetime_string
FROM partman.part_config;
```

### 4. What partitions currently exist?

```sql
SELECT tablename
FROM pg_tables
WHERE tablename LIKE 'attempts%'
ORDER BY tablename;
```

### 5. What schema is partman actually in?

```sql
SELECT nspname FROM pg_namespace
WHERE nspname IN ('partman', 'extensions');
```

### 6. Can we call maintenance manually? (test)

```sql
CALL partman.run_maintenance_proc();
```

If that errors, try:

```sql
CALL extensions.run_maintenance_proc();
```
