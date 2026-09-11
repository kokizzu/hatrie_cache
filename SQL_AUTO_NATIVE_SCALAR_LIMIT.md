# Automatic Native Scalar LIMIT/OFFSET

`hatSql` automatically selects the native scalar batch runtime for a plain
single-source projection with a finite `LIMIT` and optional `OFFSET`. The
runtime evaluates rows in source order, stops once the requested page is full,
and avoids materializing rows after the page boundary.

The automatic path requires the same conservative scalar shape as the regular
native dataflow path: one `CACHE` or `KEYS` source, an ordinary row resolver,
scalar `WHERE` and `SELECT` expressions, and no joins, unions, CTEs, grouping,
ordering, aggregates, `LIMIT BY`, windows, custom functions, or `WITH FILL`.
`LIMIT WITH TIES` remains on the established ordered executor.

For example:

```sql
FROM CACHE('items') AS src
SELECT src.id, src.value
WHERE src.value >= 0
LIMIT 16 OFFSET 32
```

The result columns and row order are unchanged. Set
`SQLQueryOptions.DisableNativeDataflow = true` to force the existing
materialized executor for compatibility testing or an A/B comparison. Observed
executions identify the selected path as `NATIVE DATAFLOW` with detail
`automatic scalar batch execution`.
