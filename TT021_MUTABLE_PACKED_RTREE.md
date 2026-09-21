# TT-021 Mutable Packed R-Tree

`PackedRTree` remains immutable and allocation-free for its existing read path.
`MutablePackedRTree[T]` adds an opt-in update layer for workloads that need
updates without rebuilding the full packed tree after every write.

## Design

- The base is a compact immutable `PackedRTree`.
- Upserts and deletes are kept in a small mutable `RTree` delta.
- Queries visit the packed base, skip dirty IDs, then visit current delta rows.
- `Compact` explicitly folds the live state into a new packed base.
- Results use packed traversal order followed by pending update order; callers
  must not depend on insertion order.
- The update layer is protected by an `RWMutex`; compaction takes the write
  lock, so callers choose when the rebuild pause occurs.

Example:

```go
tree, err := hatDataStructure.NewMutablePackedRTree([]hatDataStructure.MutableSpatialEntry[string]{
    {ID: 1, Bounds: hatDataStructure.SpatialBox{MaxX: 1, MaxY: 1}, Value: "one"},
}, hatDataStructure.PackedRTreeOptions{})
if err != nil {
    return err
}
if err := tree.Upsert(1, hatDataStructure.SpatialBox{MinX: 2, MaxX: 3, MaxY: 1}, "updated"); err != nil {
    return err
}
values, err := tree.Query(hatDataStructure.SpatialBox{MaxX: 4, MaxY: 4})
if err != nil {
    return err
}
_ = values
return tree.Compact()
```

## Benchmark

Workload: 10,000 two-dimensional entries, one update of ID 5050 and one query
covering 100 entries. Five samples were run on the same machine with
`make benchmark-tt021-mutable`; medians are used for ratios.

| Operation | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing mutable `RTree` update + query | 4,086 | 2,040 | 8 | baseline |
| Packed rebuild update + query | 5,977,552 | 547,522 | 102 | baseline |
| Linear scan update + query | 13,518 | 0 | 0 | baseline |
| `MutablePackedRTree` update + query | 1,633 | 0 | 0 | 2.50x faster than mutable `RTree`; 3.66x fewer ns than packed rebuild |
| `MutablePackedRTree` read-only query | 1,284 | 0 | 0 | 1.74x slower than immutable packed query, but remains allocation-free |
| `MutablePackedRTree` update + compact | 3,400,805 | 631,444 | 111 | 1.76x faster than packed rebuild; 1.15x transient bytes |
| Immutable `PackedRTree` read-only query | 736 | 0 | 0 | read-only reference |

Raw samples:

```text
Existing RTree update + query: 4086, 4079, 4067, 4133, 4102 ns/op
Packed rebuild update + query: 5916521, 5977552, 6300101, 6341042, 5717230 ns/op
Linear scan update + query: 11749, 12569, 13518, 13705, 14286 ns/op
Mutable packed update + query: 1633, 1655, 1451, 1688, 1535 ns/op
Mutable packed read-only query: 1271, 1312, 1314, 1238, 1284 ns/op
Mutable packed update + compact: 3448363, 3400805, 3267021, 3467334, 3210593 ns/op
Immutable packed read-only query: 743.7, 736.1, 694.7, 785.1, 702.2 ns/op
```

The first overlay prototype was rejected because it sorted and copied every
query result: it measured about 5.9 microseconds and 2,104 B/op, while
compaction reached about 10 ms and 950 KB/op. The accepted implementation
visits packed values directly, stores only dirty values, and removes duplicate
bounds from the packed base.

The feature is opt-in. Existing `PackedRTree` and `RTree` behavior is
unchanged, and callers with frequent compaction should account for the
documented transient allocation cost.
