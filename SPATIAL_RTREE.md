# R-tree Spatial Index

`hat/hatDataStructure` now provides an importable `RTree` for axis-aligned
rectangle and point lookups. This adopts the R-tree index idea used by
Tarantool for spatial candidates without changing the existing WGS84
`hatSql.GeoIndex` grid behavior.

## API

```go
tree, err := hatDataStructure.NewRTree(0) // 0 selects the default fanout
if err != nil {
	return err
}
err = tree.Upsert(42, hatDataStructure.RTreeBounds{
	MinX: 100, MinY: 200, MaxX: 110, MaxY: 210,
})
if err != nil {
	return err
}

ids, err := tree.SearchPoint(105, 205) // [42]
if err != nil {
	return err
}
_ = ids

// Reuse a caller-owned result buffer on a hot query path.
buffer := make([]uint64, 0, 32)
buffer, err = tree.SearchInto(buffer[:0], hatDataStructure.RTreeBounds{
	MinX: 0, MinY: 0, MaxX: 1000, MaxY: 1000,
})
```

`Upsert` replaces an existing ID, `Delete` removes it, and `Len` reports the
number of unique IDs. `Search` and `SearchPoint` allocate a result slice;
`SearchInto` and `SearchPointInto` append into a reusable destination and sort
only the appended IDs. Results are ascending by ID and rectangle edges are
inclusive.

Coordinates and rectangle edges must be finite, and minimum edges cannot
exceed maximum edges. The zero value of `RTree` is usable. `NewRTree` accepts
`0` for the default fanout of 16 and explicit fanouts from 4 through 256.
Searches use a read lock and mutations use a write lock, so one tree can be
shared safely by concurrent readers and writers.

## Complexity And Storage

Rectangle insertion and deletion are average-case `O(log n)` plus node splits
or condensing; a query is average-case `O(log n + k)` for `k` matches and
`O(n)` in the worst case. Results are sorted for deterministic callers, adding
`O(k log k)` work. `SearchInto` avoids result allocation once the caller's
buffer has enough capacity.

The index stores a compact fixed-shape rectangle in each leaf plus a uint64 ID
lookup map used to make replacement and deletion efficient. That map duplicates
the rectangle metadata, so the R-tree is not automatically smaller than the
existing sparse geographic grid for point-only workloads. It is useful when
rectangles overlap heavily or the query area is selective. The R-tree is a
derived index: persist the authoritative records and rebuild the index after a
restart rather than treating the in-memory tree as durable data.

This is a Cartesian coordinate index. It does not normalize longitude,
handle the international date line, or calculate great-circle distance. Use
`hatSql.GeoIndex` and its WGS84 helpers for those geographic semantics.

## Benchmark

The repository benchmark builds 10,000 half-unit rectangles and searches a
selective 11-by-11 area. The linear comparison checks every rectangle. Run it
with:

```text
make benchmark-rtree
```

Sample run on an AMD Ryzen 9 5950X, Go 1.26.6, Linux, five counts per case:

| Path | Typical ns/op | B/op | allocs/op | Relative to linear scan |
| --- | ---: | ---: | ---: | ---: |
| R-tree `Search` | 2,041 | 2,040 | 8 | 8.2x faster |
| R-tree `SearchInto` | 1,301 | 0 | 0 | 12.8x faster |
| Linear rectangle scan | 16,712 | 2,040 | 8 | 1.0x |

Observed runs varied from 2,016 to 2,083 ns/op for allocating R-tree search,
1,282 to 1,353 ns/op for reusable-buffer search, and 14,104 to 17,441 ns/op
for the linear scan. The speedup applies to selective searches; broad queries
still visit many candidates and pay the deterministic result sort. The
benchmark measures query-path allocations, not the one-time index footprint.
