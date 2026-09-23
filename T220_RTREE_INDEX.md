# T220: R-tree Spatial Index

The repository already provides a concurrent mutable `RTree` for exact
axis-aligned rectangle and point searches. It is the spatial-index answer to
the T220 inspiration item; SQL integration and packed read-mostly variants are
documented separately in [TR027_RTREE_SPATIAL_INDEX.md](TR027_RTREE_SPATIAL_INDEX.md)
and [TT021_PACKED_RTREE.md](TT021_PACKED_RTREE.md).

## API

```go
tree := NewDefaultRTree()
_ = tree.Upsert(42, RTreeBounds{
	MinX: 103.8,
	MinY: 1.2,
	MaxX: 103.9,
	MaxY: 1.3,
})

ids, err := tree.Search(RTreeBounds{
	MinX: 103.0,
	MinY: 1.0,
	MaxX: 104.0,
	MaxY: 2.0,
})
```

`Upsert` replaces an existing ID, `Delete` removes it, and `Len` reports the
number of indexed rectangles. `Search` returns intersecting IDs in ascending
ID order. `SearchPoint` is the point-query form. `SearchInto` and
`SearchPointInto` append into caller-owned buffers, preserving any prefix
already in the buffer, which makes repeated scans allocation-free when the
capacity is sufficient.

Bounds and points reject NaN, infinity, and inverted rectangles. Rectangle
edges are inclusive. The zero value is usable and selects the default node
fanout; `NewRTree` allows a fanout from 4 through 256.

## Cost Model

- R-tree pruning avoids checking every rectangle for spatially selective
  queries.
- The mutable tree keeps a reverse ID map so updates and deletes can find the
  old rectangle exactly.
- Default `Search` allocates its result; steady-state callers should reuse
  `SearchInto` buffers.
- Mutable updates are more expensive than a flat slice and retain tree nodes.
  Read-mostly workloads can use the packed immutable R-tree documented in
  `TT021_PACKED_RTREE.md`, while mutable workloads use this structure.
- Results are sorted by ID for deterministic callers, so a large result set
  pays sorting work after spatial pruning.

## Benchmark

The current benchmark indexes 10,000 half-unit rectangles and queries a small
rectangle. It uses five samples on Linux/amd64 with an AMD Ryzen 9 5950X.

| Workload | Median | Memory | Relative |
| --- | ---: | ---: | ---: |
| Linear scan | 15,913 ns/op | 2,040 B/op, 8 allocs/op | 1.0x |
| Mutable R-tree `Search` | 3,024 ns/op | 2,040 B/op, 8 allocs/op | **5.26x faster** |
| Mutable R-tree `SearchInto` | 1,458 ns/op | 0 B/op, 0 allocs/op | **10.9x faster** |

Raw direct-query samples from `make benchmark-t220` are:

```text
RTree Search:       3024, 3041, 3163, 2915, 2979 ns/op; 2040 B/op; 8 allocs/op
RTree SearchInto:   1481, 1463, 1423, 1458, 1413 ns/op; 0 B/op; 0 allocs/op
Linear scan:       16226, 15762, 15913, 17134, 15896 ns/op; 2040 B/op; 8 allocs/op
```

The result is workload-dependent: the R-tree wins on selective spatial
queries, while a packed immutable layout is a better choice when updates are
rare and memory locality dominates.
