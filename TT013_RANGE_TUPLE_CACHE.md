# TT-013: Bounded ordered range tuple cache

`hatDataStructure.OrderedIndexRangeCache` adds an opt-in Tarantool Vinyl-style
cache for repeated inclusive ranges over an existing `OrderedIndex`.

The cache uses a fixed-capacity round-robin tuple store. It compares the typed
bounds through the index comparator, stores immutable range snapshots, copies
hits into caller-provided destination capacity, and invalidates every slot
when the index generation changes. It does not add a map, string key, or
allocation to the normal `OrderedIndex` path because callers construct the
cache explicitly.

Use `RangeInto` in a hot loop:

```go
cache, err := hatDataStructure.NewOrderedIndexRangeCache(index, 8)
if err != nil {
	return err
}
scratch := make([]hatDataStructure.OrderedIndexEntry[Row, int], 0, 1024)
scratch, found := cache.RangeInto(4500, 5499, scratch[:0])
if !found {
	return nil
}
```

The cache is useful for repeated ranges such as dashboard windows, repeated
pagination, or hot interval lookups. One-shot ranges should continue using
`OrderedIndex.Range`; a cache miss intentionally retains a snapshot and is
more expensive than the existing allocation-free iterator.

## Measurement

The fixture has 10,000 ordered integer entries and scans the same 1,000-entry
inclusive range. Five benchmark samples were captured on Linux/amd64 with an
AMD Ryzen 9 5950X.

| Workload | CPU | Heap | Allocations | Compared with pre-change direct scan |
| --- | ---: | ---: | ---: | --- |
| Direct range scan before feature | 2,691 ns/op | 0 B/op | 0 | Baseline |
| Direct range scan after feature | 2,526 ns/op | 0 B/op | 0 | Existing path unchanged; 1.07x faster in this run |
| Repeated cached hit | 596.1 ns/op | 0 B/op | 0 | 4.51x faster |
| Rotating cold miss | 3,943 ns/op | 24,576 B/op | 1 | 1.47x slower; bounded retained tuple snapshot |

Raw samples:

```text
Before direct: 2691, 2663, 2515, 2731, 2694 ns/op; 0 B/op; 0 allocs/op
After direct:  2631, 2526, 2526, 2291, 2289 ns/op; 0 B/op; 0 allocs/op
Cached hit:    592.9, 596.1, 608.7, 596.3, 540.4 ns/op; 0 B/op; 0 allocs/op
Cold miss:     3943, 3358, 4169, 3836, 4130 ns/op; 24576 B/op; 1 alloc/op
```

The cache is therefore deliberately not the default. Capacity is explicit and
the `Stats` API exposes hit, miss, cached-entry, and cached-value counts so an
operator can verify that a workload is actually repetitive before retaining
range tuples.
