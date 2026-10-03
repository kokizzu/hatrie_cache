# M065 SQL Incremental Window Frames

The SQL executor now uses a bounded rolling-state fast path for `SUM` and
`AVG` windows shaped as `ROWS BETWEEN N PRECEDING AND CURRENT ROW`, including
the equivalent unbounded-preceding form. The path is automatic and preserves
the existing evaluator for `RANGE`, following bounds, frame exclusions other
than `NO OTHERS`, and unsupported expressions.

For bounded frames, only the current rolling contributions are retained. A
numeric value is added once and removed once when it leaves the frame, instead
of allocating and rescanning the entire frame for every output row. Null and
non-numeric values retain the existing aggregate behavior: they do not
contribute to `SUM` or `AVG`.

Example:

```sql
FROM CACHE('events') AS event
SELECT event.bucket,
       event.seq,
       SUM(event.value) OVER (
         PARTITION BY event.bucket
         ORDER BY event.seq
         ROWS BETWEEN 127 PRECEDING AND CURRENT ROW
       ) AS rolling_sum
```

Focused correctness, race, vet, and benchmark commands are exposed through
the repository Makefile:

```text
make test-m065-sql-window
make test-m065-sql-window-package
make benchmark-m065-sql-window
make race-m065-sql-window
make vet-m065-sql-window
```

The optimization changes execution cost only; SQL result semantics and the
default configuration remain unchanged.
