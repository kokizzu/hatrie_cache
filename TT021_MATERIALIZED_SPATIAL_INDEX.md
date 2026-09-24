# TT-021 MaterializedSource Spatial Index

`MaterializedSource.BuildSpatialIndex(latitudeField, longitudeField)` is an
opt-in point R-tree for SQL `GEO_WITHIN_BOX` and `GEO_WITHIN_RADIUS` predicates.
The default source remains a full scan, so existing callers pay no index build
or write-maintenance cost unless they enable it.

## Usage

```go
source := hatSchema.NewMaterializedSource([]hatSchema.DerivedColumn{
    {Name: "id"},
    {Name: "latitude"},
    {Name: "longitude"},
})

if _, err := source.BuildSpatialIndex("latitude", "longitude"); err != nil {
    return err
}

resolver := hatSchema.SQLResolverAdapter{
    Sources: map[string]*hatSchema.MaterializedSource{"points": source},
}
```

The SQL query shape is unchanged:

```sql
FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, -6.2088, 106.8456, 200000)
SELECT p.id
```

The index is maintained for later `Insert` and `Upsert` operations. Builds use
the source generation fence and retry if rows change while the tree is being
constructed. `HasSpatialIndex` reports whether a coordinate pair is currently
installed.

## Correctness

- The R-tree returns candidates only; SQL evaluates the original predicate
  again, including the exact great-circle radius check.
- NULL, unsupported, non-finite, and out-of-range coordinates stay in a
  fallback candidate set. This preserves ordinary SQL NULL and error behavior
  instead of silently dropping rows.
- Dateline-crossing longitude boxes are searched as two R-tree boxes.
- The feature is not persisted yet. Callers must rebuild it after restore or
  restart; persistence and replication remain open TT-021 work.

## Benchmark

Command:

```text
make benchmark-tt021-materialized-spatial-index
```

The fixture contains 20,000 rows. The timed query selects roughly 20 points
from a small latitude/longitude box. Five `-benchmem` samples ran on Linux
amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Full SQL source scan | 22,467,284 | 19,718,314 | 120,068 | Baseline |
| Warm materialized R-tree | 34,366 | 27,672 | 153 | 653.8x faster, 712.4x lower B/op, 784.8x fewer allocs |
| R-tree build | 43,363,374 | 5,496,445 | 5,959 | One-time build cost |
| Existing-row upsert without index | 2,772 | 2,180 | 20 | Write baseline |
| Existing-row upsert with index | 3,060 | 2,179 | 20 | 1.10x CPU, no allocation increase |

The build benchmark measures cumulative construction allocation, not retained
index size. Retained heap depends on the tree shape and coordinate distribution
and is not inferred from `B/op`. The query benchmark excludes the one-time
build, so the measured query path breaks even with the full scan after roughly
two queries on this fixture. The maintained write path costs about 10% CPU in
the one-row upsert fixture, with no additional allocations. Broad spatial
predicates can return many candidates and reduce the query advantage; the
index remains opt-in for that reason.

Raw samples:

```text
BenchmarkTT021MaterializedSpatialScan-32           56  21742058 ns/op 19718410 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           54  22519668 ns/op 19718308 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           52  22467284 ns/op 19718524 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           50  22702720 ns/op 19718314 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           50  21858701 ns/op 19718309 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       31570     34658 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       37408     34366 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       36838     33064 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       35542     33204 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       35299     34896 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     26  41032388 ns/op  5496457 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     24  47911718 ns/op  5496445 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     25  43014633 ns/op  5496453 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     26  43469813 ns/op  5496440 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     24  43363374 ns/op  5496440 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  464716      2915 ns/op     2180 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  473260      2745 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  392414      2722 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  466317      2772 ns/op     2180 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  429489      2918 ns/op     2180 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 375526      3129 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 371815      2936 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 417079      3019 ns/op     2180 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 377115      3060 ns/op     2180 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 401857      3156 ns/op     2179 B/op     20 allocs/op
```
