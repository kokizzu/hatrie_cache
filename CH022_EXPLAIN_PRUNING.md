# CH-022: Structured EXPLAIN pruning telemetry

`EXPLAIN ANALYZE` now exposes pruning decisions as typed data instead of
requiring operators to parse the human-readable `detail` string. The fields
are available on `hatSql.ExplainStep.Pruning` and on the tabular EXPLAIN rows.

Each pruning step reports:

- `total_rows`: rows available to the pruning operator;
- `skipped_rows`: rows rejected by the index, mark, or segment metadata;
- `scanned_rows`: rows whose values were inspected;
- `matched_rows`: rows passing the predicate in the scanned range;
- `residual_rows`: scanned rows that did not match;
- `residual_false_positive_rate`: residual rows as a percentage of scanned rows.

This covers the existing Bloom, n-gram, numeric, generic columnar, and Top-N
segment paths. It is diagnostic-only: regular queries do not create explain
metrics, and regular `EXPLAIN` continues to omit actual pruning counters.
The API is additive and the existing detail text is retained for human logs.

## Example

```go
result, err := hatSql.ExecuteSQLQueryParameters(
    ctx,
    "EXPLAIN ANALYZE SELECT id FROM CACHE('items') ORDER BY score ASC LIMIT 2",
    resolver,
    nil,
    hatSql.SQLQueryOptions{},
)
if err != nil {
    return err
}
for _, step := range result.Plan {
    if step.Pruning != nil {
        fmt.Println(step.Node, step.Pruning.SkippedRows)
    }
}
```

## Measured Cost

On an AMD Ryzen 9 5950X with the deterministic Top-N segment-pruning fixture,
the five-sample median changed from `10,173 ns/op`, `11,521 B/op`, and
`97 allocs/op` to `12,242 ns/op`, `12,174 B/op`, and `102 allocs/op`. This is a
`1.20x` CPU cost, `1.06x` allocation bytes, and five additional allocations
for EXPLAIN ANALYZE output containing the new structured data. The cost is
limited to the opt-in EXPLAIN ANALYZE path; it is not part of ordinary query
execution. Reproduce it with `make benchmark-ch022-explain-pruning`.
