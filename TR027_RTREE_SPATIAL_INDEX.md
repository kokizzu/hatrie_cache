# TR-027: Spatial R-tree SQL Index

Status: implemented and opt-in.

This incorporates Tarantool's spatial-index idea for bounded point queries. A
`RTreeSpatialSource` implements the normal SQL source contract and adds an
optional `GeoIndexedSourceResolver` path for `GEO_WITHIN_BOX` and
`GEO_WITHIN_RADIUS` predicates.

## Usage

```go
source, err := hatSql.NewRTreeSpatialSource(hatSql.RTreeSpatialSourceOptions{
    SourceName:     "CACHE",
    Name:           "points",
    LatitudeField:  "latitude",
    LongitudeField: "longitude",
})
if err != nil {
    return err
}
if err := source.Upsert("jakarta", hatSql.Row{
    "id": "jakarta", "latitude": -6.2088, "longitude": 106.8456,
}); err != nil {
    return err
}

result, err := hatSql.ExecuteSQLQueryContext(ctx, `
FROM CACHE('points') AS p
WHERE GEO_WITHIN_RADIUS(p.latitude, p.longitude, -6.2088, 106.8456, 200000)
SELECT p.id`, source, hatSql.SQLQueryOptions{})
```

Updates use the same key and deletes remove the row from the R-tree:

```go
err = source.Upsert("jakarta", updatedRow)
deleted := source.Delete("jakarta")
```

## Correctness Contract

- The index returns candidates only. SQL evaluates the original predicate
  again, including the exact great-circle distance for radius queries.
- Dateline-crossing boxes are split into two R-tree searches and de-duplicated.
- NULL, missing, non-numeric, or invalid coordinate rows remain candidates.
  This preserves SQL NULL behavior and ensures an invalid coordinate still
  produces the same evaluation error as a full scan.
- Non-spatial queries and unsupported spatial expressions use the ordinary
  source path. The source does not change result ordering guarantees beyond
  the existing no-`ORDER BY` SQL semantics.
- The source indexes one latitude/longitude pair and point bounds only. It
  does not claim polygon, nearest-neighbor, or arbitrary geometry support.

## Tradeoffs

The R-tree is opt-in because every indexed upsert performs index maintenance
and the index retains structural memory in addition to the row source. The
benchmark uses 50,000 points and a selective 100 km radius query. It measures
the existing full row scan against the R-tree candidate path on Linux/amd64,
AMD Ryzen 9 5950X, five samples per benchmark:

| Path | Median ns/op | B/op | allocs/op | Relative query time |
| --- | ---: | ---: | ---: | ---: |
| Full scan | 32,574,123 | 31,618,254 | 200,040 | 1.00x |
| R-tree candidates | 10,010 | 6,737 | 35 | 3,254x faster |

The index build benchmark for the same 50,000 rows is 146,309,264 ns/op,
39,435,496 B/op, and 158,874 allocs/op. Those are cumulative build
allocations, not a retained-heap measurement; build once and reuse the source
for the query win to amortize that cost.

Raw five-sample output (`ns/op`, `B/op`, `allocs/op`):

```text
Full scan: 30618686/31617664/200039, 32679497/31618254/200040, 32720365/31618244/200040, 32230665/31618296/200040, 32574123/31617928/200039
R-tree query: 9734/6737/35, 10024/6737/35, 9998/6737/35, 10070/6737/35, 10010/6737/35
R-tree build: 145828531/39435490/158874, 146577974/39435496/158874, 146309264/39435500/158874, 145186483/39435489/158874, 148924510/39435558/158875
```

Run the reproducible checks with:

```text
make test-tr027-spatial-index
make benchmark-tr027-spatial-index
make race-tr027-spatial-index
make vet-tr027-spatial-index
```
