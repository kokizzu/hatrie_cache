# Automatic Native Grouped SQL

`hatSql` automatically selects the existing native batch runtime for a narrow
grouped query shape. This adopts the vectorized, operator-oriented execution
style used by analytical engines while preserving the established SQL executor
as the compatibility path.

The automatic path currently requires:

- a single `CACHE` or `KEYS` source;
- an ordinary row resolver without a specialized partition, columnar, stream,
  lookup, ordered, or index contract;
- one or two direct field expressions in `GROUP BY`;
- direct group-field projections and the supported native aggregates;
- no `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`, `WITH TIES`, `LIMIT BY`, window,
  custom-function, join, union, CTE, or other richer SQL stage.

For example:

```sql
FROM CACHE('items') AS src
SELECT src.id, COUNT(*) AS total, SUM(src.value) AS total_value
GROUP BY src.id
```

The two-field form is also eligible when both group fields are projected:

```sql
FROM CACHE('items') AS src
SELECT src.region, src.tier, COUNT(*) AS total
GROUP BY src.region, src.tier
```

The native grouped operator preserves the compiled query's selected columns and
row values. Unsupported shapes fail closed to the existing materialized
executor. Set `SQLQueryOptions.DisableNativeDataflow = true` to force that
fallback for compatibility testing or an A/B comparison. Observed executions
identify the selected path as `NATIVE DATAFLOW` with detail
`automatic grouped batch execution`.
