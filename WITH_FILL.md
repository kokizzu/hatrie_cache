# SQL `WITH FILL`

SQL queries can fill missing timestamp buckets after an ordered result:

```sql
SELECT ts, SUM(value) AS total
FROM CACHE('events')
GROUP BY ts
ORDER BY ts WITH FILL
FROM TIMESTAMP '2026-01-01T00:00:00Z'
TO TIMESTAMP '2026-01-01T00:04:00Z'
STEP DURATION '1m'
```

The executor emits the supplied rows and inserts `NULL` values for selected
columns in missing buckets. `FROM` and `TO` are timestamp literals, `STEP` is
a positive duration literal, and the upper bound is exclusive. The fill column
must be selected, either directly or through a simple selected alias. Filled
rows remain subject to `LIMIT`, `OFFSET`, and the configured maximum row
budget.

The current implementation supports one `ORDER BY` item and rejects
descending fill, multiple order items, and combinations with `LIMIT BY`. It
works for materialized and streaming query APIs, including an empty source.
The fill operation is bounded by the executor's result limit, so a large time
range cannot silently allocate an unbounded result.

Filled rows are generated after grouping/projection and before the final limit
is applied. This keeps aggregate values attached to existing buckets and makes
aliases resolve against the public output column name. `WITH FILL` does not
change source data or create persistent rollups.

For generated values, add an optional policy list after `STEP`:

```sql
ORDER BY ts WITH FILL
FROM TIMESTAMP '2026-01-01T00:00:00Z'
TO TIMESTAMP '2026-01-01T00:04:00Z'
STEP DURATION '1m'
INTERPOLATE (value LINEAR, status PREVIOUS)
```

`PREVIOUS` and `NEXT` copy values from observed rows on either side of a gap.
`LINEAR` interpolates finite numeric endpoints and returns `NULL` when an
endpoint is missing or unsupported. Omitting a policy means `PREVIOUS`; the
policy applies only to generated rows. See
[`C218_WITH_FILL_INTERPOLATION.md`](C218_WITH_FILL_INTERPOLATION.md) for the
full contract and benchmark.

Focused coverage is in `hat/hatSql/with_fill_query_test.go`, including missing
buckets, empty input, alias resolution, limits, streaming output, invalid
forms, and an allocation-reporting benchmark.
