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

## Persistence

`MarshalSpatialIndex(latitudeField, longitudeField)` and
`RestoreSpatialIndex(latitudeField, longitudeField, frame)` provide an opt-in
`HSI1` binary snapshot for the maintained index. The bounded frame contains
the coordinate fields, source row count, normalized indexed points, fallback
positions, a source-membership SHA-256 digest, and a CRC32C trailer. It does
not contain source rows; restore the rows first, then restore the frame.

Restore validates the frame size and ordering, rejects checksum corruption and
source drift, and publishes the rebuilt tree atomically. Later inserts and
upserts continue to maintain the restored index. CRC32C detects accidental
corruption but is not an authenticity or encryption mechanism; callers that
send snapshots between trust boundaries need authenticated transport or an
outer signature. Durable file naming, fsync, retention, and replication remain
caller-owned.

```go
frame, err := source.MarshalSpatialIndex("latitude", "longitude")
if err != nil {
    return err
}

if err := restored.RestoreSpatialIndex("latitude", "longitude", frame); err != nil {
    return err
}
```

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
| Full SQL source scan | 24,438,944 | 19,718,420 | 120,068 | Baseline |
| Warm materialized R-tree | 33,616 | 27,672 | 153 | 727.0x faster, 712.4x lower B/op, 784.8x fewer allocs |
| R-tree build | 41,278,080 | 5,496,444 | 5,959 | One-time build cost |
| Marshal `HSI1` snapshot | 3,408,252 | 966,688 | 3 | 12.1x lower CPU than rebuild; 340,066 wire bytes |
| Restore `HSI1` snapshot | 43,918,873 | 5,815,985 | 5,962 | 1.06x rebuild CPU, 1.06x B/op, 1.00x allocs |
| Existing-row upsert without index | 2,673 | 2,179 | 20 | Write baseline |
| Existing-row upsert with index | 2,912 | 2,179 | 20 | 1.09x CPU, no allocation increase |

The build benchmark measures cumulative construction allocation, not retained
index size. Retained heap depends on the tree shape and coordinate distribution
and is not inferred from `B/op`. The snapshot benchmark includes frame decode,
source validation, and tree reconstruction but not loading source rows. It is
about 6% slower and 6% higher in transient bytes than rebuilding, with the same
allocation count; the benefit is avoiding coordinate scanning and index
construction when a valid snapshot is available. The query benchmark excludes
the one-time build, so the measured query path breaks even with the full scan
after roughly two queries on this fixture. Broad spatial predicates can return
many candidates and reduce the query advantage; the index remains opt-in for
that reason.

Raw samples:

```text
BenchmarkTT021MaterializedSpatialScan-32           54  24438944 ns/op 19718419 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           48  26011517 ns/op 19718427 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           48  21175959 ns/op 19718425 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           54  25269324 ns/op 19718420 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialScan-32           44  22876522 ns/op 19718314 B/op 120068 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       36385     33616 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       33252     36687 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       34759     32302 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       34776     33639 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndex-32       36283     31498 ns/op    27672 B/op    153 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     26  40346967 ns/op  5496452 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     27  41278080 ns/op  5496443 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     26  42079840 ns/op  5496444 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     26  41974885 ns/op  5496444 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexBuild-32     25  41239445 ns/op  5496444 B/op   5959 allocs/op
BenchmarkTT021MaterializedSpatialIndexMarshal-32  381   3408252 ns/op 99.78 MB/s 340066 wire-bytes 966688 B/op 3 allocs/op
BenchmarkTT021MaterializedSpatialIndexMarshal-32  369   3232902 ns/op 105.19 MB/s 340066 wire-bytes 966688 B/op 3 allocs/op
BenchmarkTT021MaterializedSpatialIndexMarshal-32  342   3124935 ns/op 108.82 MB/s 340066 wire-bytes 966688 B/op 3 allocs/op
BenchmarkTT021MaterializedSpatialIndexMarshal-32  354   3612903 ns/op 94.13 MB/s 340066 wire-bytes 966688 B/op 3 allocs/op
BenchmarkTT021MaterializedSpatialIndexMarshal-32  332   3520717 ns/op 96.59 MB/s 340066 wire-bytes 966688 B/op 3 allocs/op
BenchmarkTT021MaterializedSpatialIndexRestore-32   27 43918873 ns/op 7.74 MB/s 340066 wire-bytes 5815985 B/op 5962 allocs/op
BenchmarkTT021MaterializedSpatialIndexRestore-32   24 45708507 ns/op 7.44 MB/s 340066 wire-bytes 5815987 B/op 5962 allocs/op
BenchmarkTT021MaterializedSpatialIndexRestore-32   24 44932128 ns/op 7.57 MB/s 340066 wire-bytes 5815986 B/op 5962 allocs/op
BenchmarkTT021MaterializedSpatialIndexRestore-32   28 41005150 ns/op 8.29 MB/s 340066 wire-bytes 5815984 B/op 5962 allocs/op
BenchmarkTT021MaterializedSpatialIndexRestore-32   28 43969572 ns/op 7.73 MB/s 340066 wire-bytes 5815985 B/op 5962 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  501787      2593 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  416739      2677 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  398073      2673 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  437169      2662 ns/op     2180 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertScan-32  391369      2702 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 413935      2929 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 413556      2912 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 398709      2833 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 404980      2859 ns/op     2179 B/op     20 allocs/op
BenchmarkTT021MaterializedSpatialUpsertIndex-32 409430      2917 ns/op     2180 B/op     20 allocs/op
```
