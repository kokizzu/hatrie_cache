# TR-024: Covering Materialized Indexes

This is a scoped ClickHouse/Tarantool-style covering-index slice for
`hatSchema.MaterializedSource`. The SQL planner already had a
`CoveringIndexedSourceResolver` contract and fallback path; this implementation
connects that contract to materialized CACHE sources.

## Usage

Build the index after creating and populating a source:

```go
source := hatSchema.NewMaterializedSource([]hatSchema.DerivedColumn{
	{Name: "id", Identity: true},
	{Name: "region"},
	{Name: "name"},
	{Name: "payload"},
})

report, err := source.BuildCoveringIndex("region", []string{"name"})
if err != nil {
	return err
}
// report.Fields contains ["region", "name"].
_ = report
```

`BuildCoveringIndex` is online: reads and inserts continue while the source is
scanned. A concurrent insert causes the build to retry before publishing the
new immutable posting/projection set. Inserts after publication maintain the
index automatically.

For a query shaped like:

```sql
FROM CACHE('people') AS p
WHERE p.region = 'eu'
SELECT p.region, p.name
```

the adapter returns only the projected fields from the covering index. If the
requested fields are not covered, the resolver returns `available=false` and
the existing regular-index or full-scan path is used.

## Scope And Tradeoff

- One covering index is maintained per equality-predicate field. Rebuilding a
  field replaces its previous covering index atomically.
- The index is opt-in. Existing `MaterializedSource` instances and ordinary
  `DerivedColumn{Indexed: true}` indexes keep their behavior and storage cost.
- Projected row maps are retained for every indexed row. Values are shallow
  references, but the map and posting metadata add resident memory. This is the
  main cost and is why the feature is not enabled automatically.
- The current source is insert-only; callers that add update/delete semantics
  must rebuild or extend the maintenance contract before using this path.
- Only simple CACHE equality predicates with direct field projections use the
  covering path. Other query shapes retain their existing planner behavior.

## Verification

The focused tests cover online installation, insert maintenance, projected-row
isolation, regular lookup fallback, resolver fallback, and end-to-end SQL
execution. Race and package tests pass through the Makefile targets listed in
the benchmark report.

## Benchmark

The workload contains 20,000 rows, 16 extra payload fields, 64 region values,
and selects two fields for one equality predicate. Five-sample medians on the
same Linux/amd64 host were:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing indexed full-row path | 518,327 | 567,125 | 1,909 |
| Covering index path | 156,888 | 284,132 | 1,283 |
| Covering improvement | 3.30x faster | 2.00x lower | 1.49x fewer |

`B/op` is transient query allocation, not retained index memory. The retained
projected maps are the explicit space-for-query-cost tradeoff described above.
Raw samples, the pre-change baseline, and the latest re-verification are in
[BENCHMARK.md](BENCHMARK.md#tt-019-covering-secondary-indexes).
