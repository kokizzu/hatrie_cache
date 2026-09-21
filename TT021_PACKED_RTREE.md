# TT-021 Packed R-tree Spatial Index

`hatDataStructure.PackedRTree[T]` is an opt-in immutable two-dimensional
axis-aligned bounding-box index. It follows the useful part of Tarantool's
RTREE idea while keeping the implementation compact: entries are copied once,
bulk sorted into spatially coherent leaves, and traversed through integer
node ranges instead of pointer-linked nodes.

## Use

```go
entries := []hatDataStructure.SpatialEntry[uint64]{
	{
		Bounds: hatDataStructure.SpatialBox{MinX: 10, MinY: 20, MaxX: 12, MaxY: 22},
		Value: 42,
	},
}

index, err := hatDataStructure.NewPackedRTree(entries, hatDataStructure.PackedRTreeOptions{})
if err != nil {
	// Invalid or non-finite coordinates are rejected.
	}

query := hatDataStructure.SpatialBox{MinX: 11, MinY: 21, MaxX: 11, MaxY: 21}
matches, err := index.Query(query)
```

`SpatialBox` intersection is inclusive on all four boundaries. Coordinates
must be finite, and each minimum must be no greater than its corresponding
maximum. `NewPackedRTree` copies the input entries, so later caller mutation
cannot corrupt the index.

Use `QueryInto` with a reused destination when the result slice should not be
allocated on every query:

```go
destination := make([]uint64, 0, 64)
destination, err = index.QueryInto(query, destination)
```

Use `Visit` when a callback or early stop is preferable to materializing
results. Returning `false` stops traversal after the current match. `Query`
is the convenient allocating form; `Len` returns the indexed entry count.

`LeafSize` controls both leaf size and internal fan-out. Zero selects
`DefaultPackedRTreeLeafSize` (16). The tree is immutable: rebuild it after
adding, removing, or changing entries. The index is not automatically wired
into SQL planning, persistence, or replication; callers can map values to
row IDs and select when rebuilding is worthwhile.

## Tradeoffs

The build owns a copy of the entries and performs sorting, so it is more
expensive than retaining an unsorted slice. The benefit is fast repeated
bounding-box reads with no query allocations when `QueryInto` or `Visit` is
used. The implementation currently supports 2D boxes only and does not
provide mutable insert/delete operations.

The focused 10,000-entry benchmark measured 547,523 B/op and 102 allocations
per build. This is cumulative build allocation, not a claim that every byte
remains live after temporary sort state is released. QueryInto and Visit both
measured 0 B/op and 0 allocations/op.

See the raw five-run measurements in [BENCHMARK.md](BENCHMARK.md#tt-021-packed-r-tree-spatial-index).
