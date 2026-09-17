# M-U13 SQL Cardinality Estimates

`EXPLAIN` and `EXPLAIN ANALYZE` can expose bounded cardinality estimates for
source scans, filters, joins, and aggregates. This adopts the useful part of
Materialize-style dataflow introspection without making estimates part of
query correctness or execution decisions.

## Supported Estimates

- `SCAN` uses `SourceCardinalityResolver` when the source provides a current
  row count. `VALUES` keeps its exact literal row count.
- A simple equality filter on a `CACHE` source uses the existing exact
  `IndexValueEstimator` result, or the indexed `Rows / DistinctKeys` estimate
  when only `JSONIndexStatsResolver` is available.
- `INNER` and `LEFT` equality joins use the right-side indexed average posting
  size. `CROSS JOIN` uses the product of known input cardinalities.
- A grouped aggregate over direct `CACHE` fields uses indexed distinct-key
  statistics, including one additional group when indexed NULL rows exist.
- An aggregate without `GROUP BY` estimates one output row, including an empty
  input, matching SQL aggregate semantics.

Unsupported predicates, stale or malformed metadata, outer join shapes without
a safe estimate, and expressions without indexed statistics omit
`estimated_rows`; the planner never invents a selectivity constant.

## Explain Analyze

`EXPLAIN ANALYZE` captures estimates before execution and attaches them to the
matching observed operator. Each matched step reports:

- `estimated_rows`
- `actual_input_rows` and `actual_output_rows`
- `estimate_error_rows`
- `estimate_error_percent` when the estimate is non-zero

Metadata callbacks are advisory. Errors, unavailable metadata, and negative
counts are ignored, while source rows are still resolved normally. Regular
`EXPLAIN` does not materialize source rows, and no row values or predicate
values are included in the estimate metadata.

## Example

```go
result, err := hatSql.ExecuteSQLQuery(
    "EXPLAIN ANALYZE FROM CACHE('orders') AS o WHERE o.status = 'open' SELECT o.id",
    resolver,
)
```

For a source estimate of 3 rows, an indexed equality estimate of 2 rows, and
one observed match, the `SCAN` step reports `3` estimated and actual output
rows. The `FILTER` step reports `2` estimated rows, `1` actual output row,
`-1` estimate error rows, and `-50` percent error.

## Cost And Compatibility

The feature is explain-only. Existing resolvers that do not implement optional
metadata contracts keep estimates omitted and query results unchanged. The
focused benchmark in [BENCHMARK.md](BENCHMARK.md#mu-013-sql-cardinality-estimates)
measures the regular explain path before and after the metadata lookup.
