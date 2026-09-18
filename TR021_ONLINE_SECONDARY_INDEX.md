# TR-21a: MaterializedSource Online Secondary-Index Build

`hatSchema.MaterializedSource` previously maintained equality indexes only for
columns created with `DerivedColumn.Indexed: true`. This addition provides an
explicit `BuildSecondaryIndex` operation for an existing source, inspired by
Tarantool's online secondary-index build boundary.

This is a scoped extension, not a claim that every storage engine now supports
online index alteration. The broader TR-21 idea remains open; SQL JSON index
rebuild workers already cover the separate T020/T021 path.

## Usage

```go
source := hatSchema.NewMaterializedSource([]hatSchema.DerivedColumn{
	{Name: "id"},
	{Name: "region"},
	{Name: "payload"},
})

report, err := source.BuildSecondaryIndex("region")
if err != nil {
	return err
}
// report.Rows is the indexed snapshot size; report.Attempts is normally 1.

adapter := hatSchema.SQLResolverAdapter{
	Sources: map[string]*hatSchema.MaterializedSource{"people": source},
}
```

Before the build, `HasIndex("region")` is false and the SQL adapter reports
the index as unavailable. After publication, `Lookup` and SQL equality
predicates use the maintained index. Later `Insert` calls update the index
under the source lock.

## Publication protocol

The builder validates the declared column, copies only the row-slice headers
under a read lock, and constructs the posting map after releasing the lock.
Before publication it checks a monotone source generation. A concurrent insert
causes a retry, so no inserted row can be lost between the build snapshot and
the installed index. Publication replaces the field's posting map in one
locked operation. No background goroutine starts and no default source pays the
build cost.

The operation is intentionally synchronous. A caller that needs scheduling,
progress, cancellation, or persistence can run it through the existing index
rebuild queue and its own lifecycle policy.

## Tradeoffs

- The index retains posting positions plus its key map. Retained memory depends
  on cardinality and key size; `B/op` below measures build allocations, not the
  long-lived index footprint.
- A hot writer can force retries and repeat the build CPU. This preserves
  correctness and avoids blocking writes for the expensive scan.
- `MaterializedSource` currently exposes inserts, not row deletes or updates;
  future mutation APIs must maintain or invalidate this index explicitly.
- Existing `DerivedColumn.Indexed: true` behavior remains unchanged.

## Benchmark

The workload contains 10,000 rows and builds an index over `region` with 64
distinct values. Linux/amd64, AMD Ryzen 9 5950X, Go `-benchmem`, five samples.
The public-scan control clones every row through `Rows()` before rebuilding the
posting map.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Public scan control, paired final run | 5,079,753 | 4,653,758 | 30,613 | baseline |
| `BuildSecondaryIndex`, paired final run | 1,720,430 | 1,291,623 | 10,611 | 2.95x faster, 3.60x less bytes, 2.89x fewer allocations |

Raw paired samples:

```text
BenchmarkTT021LegacyPublicScanIndexBuild: 5079753, 5085857, 4999184, 5042269, 5195941 ns/op; 4653696-4653775 B/op; 30612-30613 allocs/op
BenchmarkTT021OnlineSecondaryIndexBuild: 1728480, 1720430, 1694304, 1680027, 1770619 ns/op; 1291623-1291634 B/op; 10611-10612 allocs/op
```

The first pre-change baseline, before adding the API, had a `5,828,411 ns/op`
median with `4,653,742 B/op` and `30,612 allocs/op`. Reproduce the paired run
with:

```text
make benchmark-tr021-online-secondary-index
```

Correctness and race verification:

```text
make test-tr021-online-secondary-index
make race-tr021-online-secondary-index
make test-tr021-sql-package
make vet-tr021-online-secondary-index
```
