# C237: Projection Selection Explain

`EXPLAIN` now reports materialized projection selection when the caller opts in
with `QueryOptions.ProjectionCatalog`. The ordinary query path is unchanged;
the catalog is consulted for explain diagnostics only.

The plan contains a `PROJECTION` step with one candidate per registered
projection. Each candidate includes:

- `selected`: whether the exact, fresh candidate would be used;
- `rejected_reason`: a stable reason such as `query_not_exact`,
  `source_version_changed`, `source_version_unavailable`,
  `collation_mismatch`, `index_hint_active`, or `not_selected`;
- `estimated_rows`: retained result rows;
- `estimated_bytes`: logical encoded row-payload size used by the existing SQL
  result accounting; and
- `estimated_io_bytes`: the logical row payload expected to be read for a hit.

`estimated_io_bytes` is intentionally a logical estimate. It does not claim to
predict filesystem reads, compression, page-cache behavior, or remote object
store traffic.

Example:

```go
result, err := hatSql.ExecuteSQLQueryContext(
    ctx,
    "EXPLAIN FROM CACHE('events') SELECT name",
    resolver,
    hatSql.QueryOptions{ProjectionCatalog: views},
)
```

The same data is available in `result.Plan` and the tabular `projections`
column. Projection diagnostics are bounded by the number of registered views;
they retain no additional catalog state.

## Measurement

The benchmark runs the same exact explain query with one two-row projection
catalog, five runs on an AMD Ryzen 9 5950X:

| State | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Before C237 | 7,151 median | 8,600 | 31 |
| After C237 | 9,897 median | 11,138 | 49 |
| Change | 1.38x CPU | 1.30x bytes | 1.58x allocations |

This is an explain-only diagnostic cost. No projection catalog means the
existing explain path remains unchanged, and normal non-EXPLAIN execution does
not enumerate projection candidates.
