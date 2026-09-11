# SQL Temporal Validity

`VALID_AT(at, valid_from, valid_to)` is a first-class SQL predicate for rows
whose validity interval contains a point in time.

```sql
SELECT id, status
FROM CACHE('orders')
WHERE VALID_AT(
  TIMESTAMP '2026-01-01T12:00:00Z',
  valid_from,
  valid_to
)
```

The interval is half-open: `[valid_from, valid_to)`. `valid_from IS NULL`
means unbounded below, and `valid_to IS NULL` means unbounded above. Therefore,
the equivalent predicate is:

```sql
(valid_from IS NULL OR valid_from <= at)
AND (valid_to IS NULL OR at < valid_to)
```

`at` may be a timestamp value or supported timestamp text. The bound columns
may contain timestamp values, supported timestamp text, or `NULL`. A `NULL`
`at` produces SQL `NULL`, so it does not pass a `WHERE` clause. Exactly three
arguments are required; invalid timestamp values return the normal SQL
diagnostic.

For literal and field arguments, `VALID_AT` is recognized as a scalar-safe
builtin by the automatic native SQL dataflow planner. This keeps the common
row-filter path on the same low-allocation executor as ordinary comparisons.
Computed arguments retain the existing general expression fallback. The
predicate does not yet create or consult a physical validity index, and it does
not claim frontier-aware partition pruning; those require a provider-level
temporal index contract and remain future work.

Focused correctness and race checks:

```text
make test-mz009-temporal-validity
make test-race-mz009-temporal-validity
```

The benchmark and raw samples are recorded in
[BENCHMARK.md#mz-009-temporal-validity-filters](BENCHMARK.md#mz-009-temporal-validity-filters).
