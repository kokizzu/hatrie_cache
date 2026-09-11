# Automatic Native Grouped Top-N SQL

`hatSql` automatically selects the existing native grouped Top-N runtime for a
narrow grouped query with a finite ordered page. This fuses grouped reduction,
`HAVING`, and bounded Top-N selection without materializing and sorting every
group through the general SQL executor.

The automatic path currently requires:

- a single `CACHE` or `KEYS` source;
- an ordinary row resolver without a specialized partition, columnar, stream,
  lookup, ordered, or index contract;
- exactly one direct field in `GROUP BY`;
- a finite `LIMIT` and at least one unqualified selected-field or selected-alias
  order key;
- supported grouped aggregates and a scalar `HAVING` expression that the
  native rewriter can resolve from selected aggregate expressions;
- no `DISTINCT`, `WITH TIES`, `LIMIT BY`, `WITH FILL`, window, custom-function,
  join, union, CTE, or other richer SQL stage.

For example:

```sql
FROM CACHE('items') AS src
SELECT src.id, COUNT(*) AS total
GROUP BY src.id
HAVING COUNT(*) > 1
ORDER BY total DESC, id ASC
LIMIT 10 OFFSET 20
```

The native path retains stable order and applies `OFFSET` after grouped
selection. Qualified source-field ordering, missing or ambiguous selected
order fields, alias-only `HAVING` forms that cannot be rewritten, and other
unsupported shapes fail closed to the established materialized executor.
Set `SQLQueryOptions.DisableNativeDataflow = true` to force that fallback for
compatibility testing or an A/B comparison. Observed executions identify the
selected path as `NATIVE DATAFLOW` with detail
`automatic grouped ordered top-N batch execution`.
