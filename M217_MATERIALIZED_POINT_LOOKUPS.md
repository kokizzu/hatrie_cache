# M217: Materialized-View Point Lookups

## What Changed

`MaterializedViewDefinition.PointLookupFields` is an opt-in list of output
columns for which `MaterializedViews` builds maintained point postings. The
new `MaterializedViews.PointLookup` method returns complete cloned rows for an
indexed value:

```go
views := hatSql.NewMaterializedViews()
_, err := views.Create(ctx, hatSql.MaterializedViewDefinition{
	Name:              "people_view",
	Query:             "FROM CACHE('people') SELECT id, region, name",
	Dependencies:      []string{"people"},
	PointLookupFields: []string{"id", "region"},
}, resolver, hatSql.QueryOptions{})

rows, available, err := views.PointLookup("people_view", "region", "sg")
```

`available` is false when the view or field has no configured point index, so
callers can fall back to `Get` plus a scan. The default definition has no point
lookup fields and preserves the previous behavior and storage path.

The index stores row ordinals, not another copy of each row. The complete rows
remain in the maintained snapshot and are cloned only for the lookup result.
Duplicate values preserve snapshot order. NULL, booleans, strings, byte slices,
integer and floating-point scalar values, and `time.Time` are supported. A
configured output field that contains an unsupported value type rejects the
create or refresh instead of silently returning an incomplete index.

## Atomic Refresh Semantics

Index construction happens before a new snapshot is published. Refresh swaps
the snapshot and its point postings in the same locked publication step. If
query execution or index construction fails, both the old rows and the old
index remain visible. Unknown configured output fields are rejected during
creation.

`make m217-test` verifies complete rows, duplicate-key ordering, NULL lookup,
defensive cloning, successful refresh replacement, failed-refresh retention,
and invalid output-field rejection.

## Read Benchmark

Workload: five `-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X,
20,000 maintained rows, 100 region values, and about 200 matches for the
probed value. The baseline uses the existing public `Get` method and scans the
complete returned snapshot. The indexed path probes postings and clones only
the matching complete rows.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| `Get` plus full snapshot scan | 7,228,928 | 6,883,942 | 40,004 | 1.00x | 1.00x |
| `PointLookup` | 53,099 | 69,008 | 402 | 0.0073x (`136.1x` faster) | 0.0100x (`99.8x` lower) |

Raw samples:

```text
BenchmarkM217MaterializedViewPointLookupIndexed:     53099 69008 402; 54858 69008 402; 52117 69008 402; 51257 69008 402; 54544 69008 402
BenchmarkM217MaterializedViewPointLookupSnapshotScan: 7095357 6883941 40004; 7228928 6883944 40004; 6622637 6883939 40004; 7395002 6883946 40004; 7446474 6883942 40004
```

## Refresh Cost

The index is not free. The same five-sample workload refreshes the complete
20,000-row view on every iteration:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Refresh without point postings | 19,094,342 | 20,657,412 | 120,023 | 1.00x | 1.00x |
| Refresh with `region` postings | 24,844,656 | 21,397,197 | 140,938 | 1.301x (`30.1%` higher) | 1.036x (`3.6%` higher) |

Raw samples:

```text
BenchmarkM217MaterializedViewRefreshWithoutPointLookup: 19094342 20657492 120024; 17443675 20657391 120023; 19306441 20657405 120023; 18663410 20657494 120023; 21314782 20657412 120024
BenchmarkM217MaterializedViewRefreshWithPointLookup:    24887820 21397197 140938; 24165096 21397202 140937; 22023863 21397182 140937; 26405968 21397207 140938; 24844656 21397195 140938
```

Use this only for read-heavy maintained views or when the saved scan work is
worth the refresh cost. The benchmark reports transient allocation; retained
index memory is the posting maps and row ordinals in addition to the existing
snapshot. `MaterializedViews.Usage` continues to account logical snapshot rows
and bytes, not the in-process map overhead.

Run the focused checks with `make m217-test`, `make m217-race`, and
`make m217-vet`; reproduce all measurements with `make m217-benchmark`.
