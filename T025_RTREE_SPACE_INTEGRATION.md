# T-U25 R-tree Space Integration

`hatSchema.MaterializedSource` can now build an opt-in named point R-tree and
expose it through `SQLResolverAdapter`. This connects the existing
`hatSql.RTreeSpatialSource` implementation to a materialized CACHE space.

## Usage

```go
source := hatSchema.NewMaterializedSource([]hatSchema.DerivedColumn{
    {Name: "id"},
    {Name: "latitude"},
    {Name: "longitude"},
})

_, err := source.BuildRTreeIndex("geo", hatSchema.RTreeIndexOptions{
    LatitudeField:  "latitude",
    LongitudeField: "longitude",
})
if err != nil {
    return err
}

resolver := hatSchema.SQLResolverAdapter{
    Sources: map[string]*hatSchema.MaterializedSource{"places": source},
}
result, err := hatSql.ExecuteSQLQueryContext(ctx, `
FROM CACHE('places') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, -6.2088, 106.8456, 200000)
SELECT p.id ORDER BY p.id`, resolver, hatSql.SQLQueryOptions{})
```

The index is maintained for every insert after publication. `DropIndex("geo")`
removes the maintained R-tree without removing rows. A name must not collide
with a column, equality index, covering index, functional index, or another
R-tree. The default R-tree fanout is used when `MaxEntries` is zero.

## Correctness

The R-tree only produces candidates. The SQL executor evaluates the original
`GEO_WITHIN_BOX` or `GEO_WITHIN_RADIUS` predicate on those candidates, so
additional predicates, NULL coordinates, invalid coordinates, dateline
wrapping, and SQL comparison semantics remain authoritative. Rows whose
coordinates cannot be indexed stay in the spatial source fallback set.

The current `MaterializedSource` is append-only, so this integration maintains
inserts and does not claim update/delete support. The focused test covers build,
SQL routing, post-build insert visibility, invalid definitions, drop, and race
behavior.

## Benchmark

The recorded run uses 50,000 rows on Linux/amd64 with an AMD Ryzen 9 5950X.
The query selects a 100 km radius around `(0, 0)`.

| Operation | Time | Heap | Allocs | Comparison |
| --- | ---: | ---: | ---: | --- |
| Full scan query | 44,476,466 ns/op | 49,216,527 B/op | 300,029 | baseline |
| R-tree query | 7,585 ns/op | 6,504 B/op | 34 | 5,864x faster; 7,567x lower heap; 8,824x fewer allocations |
| Insert without R-tree | 885.7 ns/op | 762 B/op | 8 | baseline |
| Insert with R-tree | 4,342 ns/op | 1,585 B/op | 12 | 4.90x slower; 2.08x higher heap; 1.50x allocations |
| R-tree build for 50,000 rows | 123,657,854 ns/op | 37,346,156 B/op | 165,981 | one-time build cost |

The query win is large for selective spatial reads, but the write and build
costs are real. The index therefore remains explicitly opt-in; workloads that
are write-heavy or do not issue selective spatial predicates should leave it
disabled. Reproduce the run with `make benchmark-t025`. Raw output is in
`T025_BENCHMARK_RAW.txt`.
