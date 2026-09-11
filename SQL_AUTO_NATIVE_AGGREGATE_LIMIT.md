# Automatic Native Aggregate LIMIT/OFFSET

`hatSql` automatically selects the native aggregate batch runtime for global
aggregate queries with a `LIMIT` or `OFFSET`. A global aggregate produces at
most one output row, so the window is applied after reduction without carrying
the general executor's materialized intermediate rows.

The automatic path requires a single `CACHE` or `KEYS` source, an ordinary row
resolver, supported global aggregates, and no grouping, ordering, `HAVING`,
`LIMIT BY`, `WITH TIES`, windows, custom functions, joins, unions, CTEs, or
`WITH FILL`. `LIMIT 0` and any positive `OFFSET` correctly produce an empty
result after aggregate computation.

For example:

```sql
FROM CACHE('items') AS src
SELECT COUNT(*) AS total, SUM(src.value) AS total_value
LIMIT 1
```

Set `SQLQueryOptions.DisableNativeDataflow = true` to force the existing
materialized executor for compatibility testing or an A/B comparison. Observed
executions identify the selected path as `NATIVE DATAFLOW` with detail
`automatic aggregate window batch execution`.
