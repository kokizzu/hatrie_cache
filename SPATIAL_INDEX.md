# Spatial Index

`hatSql.GeoIndex` keeps the public point, bounding-box, radius, update, and
delete API unchanged. Points are assigned to one configurable geographic grid
cell. Queries enumerate cells for narrow boxes and enumerate only occupied
cells when a box would scan more than eight grid cells per occupied bucket.

Each point has exactly one bucket, so query candidate collection does not need a
per-query string set. Exact latitude/longitude filtering and sorted IDs remain
unchanged. Dateline-crossing boxes and the coarsest `360` degree grid are
covered by regression tests.

## Measurement

Command: `make benchmark-geospatial-index` on the repository benchmark host.
The three samples below are representative runs before and after the change.

| Workload | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| 10,000-point candidate collection | 107,108 ns/op, 63,624 B/op, 16 allocs/op | 56,847 ns/op, 18,752 B/op, 8 allocs/op | 1.89x faster, 70.5% fewer bytes, 50% fewer allocs |
| Sparse full-world box | 1,046,567 ns/op, 16 B/op, 1 alloc/op | 150.5 ns/op, 16 B/op, 1 alloc/op | about 6,954x faster |

The full RTREE design remains a separate checklist item. This change keeps the
lower-overhead grid representation and adds an adaptive sparse-query path.

Focused correctness, race, and package tests are available through:

- `make test-geospatial-index`
- `make race-geospatial-index`
- `make test-geospatial-all`
- `make benchmark-geospatial-index`
