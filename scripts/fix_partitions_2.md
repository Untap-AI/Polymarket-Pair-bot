## Check default partition contents

```sql
SELECT
    COUNT(*) as cnt,
    MIN(ts) as min_ts,
    MAX(ts) as max_ts
FROM public.attempts_part_default;
```
