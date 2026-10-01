# T-U23 Automatic Tuple Multikey Indexes

`hatDataStructure.TupleMultikeyIndex` is an opt-in Tarantool-style multikey
primitive for rows whose indexed value has multiple array dimensions. Each
dimension is a `[]string`; the index expands the Cartesian product and maps
each complete tuple to sorted row IDs.

```go
index := hatDataStructure.NewTupleMultikeyIndex(hatDataStructure.TupleMultikeyIndexOptions{
	MaxCombinationsPerItem: 16,
	MaxItems:               100_000,
})

err := index.Set(42, [][]string{
	{"region-sg", "region-jp"},
	{"hot", "cold"},
})
// Indexed tuples: (region-sg, hot), (region-sg, cold),
//                 (region-jp, hot), (region-jp, cold).

rows := index.Lookup([]string{"region-sg", "hot"}, nil)
// rows == []uint64{42}
```

## Semantics

- `Set` replaces every tuple for one row atomically. A failed expansion-limit
  or item-limit check leaves the old postings unchanged.
- Values inside each dimension are sorted and deduplicated. Already sorted,
  unique dimensions use a no-copy normalization fast path; caller slices are
  never retained after `Set` returns.
- An empty dimension produces no tuples. Setting `nil` or an empty dimension
  therefore clears the row.
- `Lookup` and `Contains` use the complete tuple. Row IDs are returned in
  ascending order, and `Lookup` accepts a reusable destination buffer.
- Tuple keys use length-prefixed binary encoding, so embedded NUL bytes and
  delimiter-like values cannot alias another tuple.
- The index is safe for concurrent reads and writes. It is in-memory only;
  callers own persistence, recovery, and SQL planner integration.

`MaxCombinationsPerItem` is the Cartesian-product safety bound. A nonpositive
value means unlimited subject to available memory; production callers should
set a finite value. `MaxItems` bounds rows with at least one expanded tuple,
and `MaxTupleMultikeyDimensions` is a hard 64-dimension guard against abusive
input shape.

## Measured Tradeoff

The benchmark uses 10,000 rows, two dimensions, two values per dimension, a
reusable lookup destination, and five `-benchmem` samples on an AMD Ryzen 9
5950X. The manual baseline is a delimiter-joined `map[string][]uint64` that
does not maintain reverse state, sort postings, or reject delimiter collisions.

| Operation | Manual baseline | TupleMultikeyIndex | Relative |
| --- | ---: | ---: | ---: |
| Build | 2.84 ms/op; 2.49 MB/op; 60,472 allocs/op | 5.86 ms/op; 5.44 MB/op; 67,010 allocs/op | 2.06x slower; 2.19x bytes; 1.11x allocs |
| Lookup | 121.0 ns/op; 16 B/op; 1 alloc/op | 142.4 ns/op; 48 B/op; 1 alloc/op | 1.18x slower; 3x bytes; same allocs |

The candidate intentionally does more work because it provides collision-safe
tuple encoding, sorted postings, exact reverse maintenance for update/delete,
deduplication, atomic bounds, and concurrent access. It is not a replacement
for the existing flat string index or a default SQL index. Use it when those
lifecycle and nested-array semantics are worth the measured cost.

Run the focused contract and benchmark targets with:

```text
make test-t-u23
make benchmark-t-u23
```
