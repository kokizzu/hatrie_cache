# Reusable Bounded Vector Search

`hatDataStructure.VectorIndex.SearchInto` is an opt-in search API for callers
that repeatedly request a bounded number of nearest vectors. It writes the
same descending cosine-score and ascending-ID ordering as `Search` into a
caller-owned destination slice.

```go
scratch := make([]hatDataStructure.VectorMatch, 0, 10)
matches, err := index.SearchInto(scratch, query, 10, filter)
if err != nil {
	return err
}
use(matches)
```

When `limit` is smaller than the index, the scan retains only the current top
`limit` matches in a min-heap. For a full result request it appends all matches
and sorts them. `slices.SortFunc` keeps the reusable path allocation-free when
the destination has enough capacity. The existing `Search` method and its
independent-result semantics are unchanged.

`make benchmark-vector-search` measured ten `-benchmem` samples over 10,000
eight-dimensional vectors:

| Workload | Existing `Search` median | Reusable `SearchInto` median | Result | Heap / allocations |
| --- | ---: | ---: | --- | --- |
| `LIMIT 10` | 1,780,621 ns/op | 160,016 ns/op | 11.1x faster | 977,328 B / 18 -> 0 B / 0 |
| `LIMIT 10,000` | 1,696,887 ns/op | 928,903 ns/op | 1.83x faster | 245,856 B / 4 -> 0 B / 0 |

The zero-allocation result requires the caller to retain a destination with
capacity at least `limit`; a smaller destination grows once. The method is
opt-in because changing `Search` to use a bounded heap would alter its existing
allocation and result-ownership behavior.
