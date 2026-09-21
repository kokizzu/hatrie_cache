# C237: Projection Selection Explain Output

C237 adds ClickHouse-style projection-selection diagnostics to opt-in SQL
`EXPLAIN` output. When `QueryOptions.ProjectionCatalog` is set, the plan can
show the exact materialized projection candidate, freshness decision, source
row/byte estimate, projection row/byte estimate, estimated read bytes, and
estimated bytes saved.

```go
views := hatSql.NewMaterializedViews()
// Create and refresh views as usual, with explicit source dependencies.

result, err := hatSql.ExecuteSQLQueryContext(
    ctx,
    "EXPLAIN FROM CACHE('orders') SELECT id",
    resolver,
    hatSql.QueryOptions{ProjectionCatalog: views},
)
```

The returned `QueryResult.Plan` contains a `PROJECTION SELECTION` step. Its
`Projection` field is `SQLProjectionDiagnostics`:

- `Name`, `Selected`, and `Reason` explain the candidate decision.
- `SourceRows` and `SourceBytes` describe the source when the resolver also
  implements `SQLSourceSizeResolver`.
- `ProjectionRows` and `ProjectionBytes` describe the retained projection.
- `EstimatedReadBytes` and `EstimatedSavedBytes` describe the selected read
  path using logical encoded-row estimates, not process RSS.

`SQLSourceSizeResolver` is optional. Without it, the candidate and freshness
decision are still returned, but source-side byte comparison is omitted. A
stale source version is never selected. An index hint disables projection
selection, matching normal execution. The regular query path and its existing
projection lookup are unchanged unless a caller explicitly supplies the
projection catalog.

The diagnostics are available for non-pipeline `EXPLAIN`; `EXPLAIN PIPELINE`
continues to return the pipeline-specific output. The path snapshots only
metadata and uses a non-allocating estimate when the materialized-view registry
does not have exact byte accounting.

## Measured Cost

Five `-benchmem` samples ran on Linux amd64 with an AMD Ryzen 9 5950X over a
128-row query. The control does not provide a projection catalog.

| Path | Raw ns/op samples | Median ns/op | B/op | allocs/op | Relative result |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing `EXPLAIN` control | 9416; 8953; 9349; 8164; 9335 | 9335 | 9112 | 31 | 1.00x |
| C237 projection diagnostics | 26782; 27830; 25774; 24986; 25411 | 25774 | 10370 | 47 | 2.76x CPU, +13.8% bytes, +16 allocs |

An earlier implementation cloned every retained row and measured JSON bytes on
each explain call (`196.8 us`, `91.9 KB`, `1584 allocs` median). It was replaced
before adoption with metadata-only snapshots and allocation-free estimates.
