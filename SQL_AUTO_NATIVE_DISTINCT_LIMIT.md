# Automatic Native DISTINCT LIMIT/OFFSET

`hatSql` automatically selects the native distinct batch runtime for unordered
direct-field `DISTINCT` queries with a finite first-seen page. The runtime keeps
typed membership state, skips the requested unique `OFFSET`, and stops after
the requested number of unique values instead of materializing every distinct
row.

The automatic path supports one or two direct fields from a single `CACHE` or
`KEYS` source over an ordinary row resolver. It preserves source order and
supports SQL NULL, integer, and string key values. Ordered distinct queries,
`WITH TIES`, `LIMIT BY`, grouping, windows, custom functions, joins, unions,
CTEs, and other richer shapes remain on the established executor.

For example:

```sql
FROM CACHE('items') AS src
SELECT DISTINCT src.value
LIMIT 16 OFFSET 32
```

The compiled query is not mutated while consuming the offset, so repeated and
concurrent executions use the same page. Set
`SQLQueryOptions.DisableNativeDataflow = true` to force the existing
materialized executor for compatibility testing or an A/B comparison. Observed
executions identify the selected path as `NATIVE DATAFLOW` with detail
`automatic distinct window batch execution`.
