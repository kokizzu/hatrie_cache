# T219: Typed HASH Index

`HashIndex[T, K]` provides expected constant-time exact-match lookup for a
derived key. It is a standalone secondary index in `hatDataStructure`; it does
not replace `OrderedIndex` or `MultiPartTreeIndex`, which remain the choices for
ordered scans, prefixes, and ranges.

## API

```go
type User struct {
	ID    uint64
	Email string
}

index, err := NewHashIndex(
	func(user User) string { return user.Email },
	HashIndexOptions{Unique: true, Capacity: 10_000},
)
if err != nil {
	panic(err)
}

_ = index.Upsert(42, User{ID: 42, Email: "user@example.com"})
entry, ok := index.LookupOne("user@example.com")
```

The key type `K` must be comparable because it is used as a Go map key. The
index keeps a reverse ID map so an upsert can replace the derived key and a
delete can remove the old posting exactly.

`HashIndexOptions.Unique` selects the conflict policy:

- Unique indexes keep one ID per key and reject a conflicting upsert before
  changing either row.
- Non-unique indexes keep a compact posting list and return matching IDs in
  ascending stable-ID order.

The public operations are `Upsert`, `Delete`, `LookupOne`, `Lookup`,
`LookupInto`, `LookupIDs`, `LookupIDsInto`, `Contains`, `Len`,
`DistinctKeys`, and `Clear`. The `Into` variants reset and reuse caller-owned
result slices so repeated queries can remain allocation-free.

All operations use the index mutex and are safe for concurrent access. The
extractor runs before the write lock; callers should keep it deterministic and
side-effect free.

## Benchmark

The reproducible benchmark uses 10,000 integer records and compares the HASH
index with the existing `FunctionalIndex` on the same process and workload.
It uses three `-benchmem` samples on Linux/amd64 with an AMD Ryzen 9 5950X.

| Workload | HASH | Functional index | Relative result |
| --- | ---: | ---: | ---: |
| Unique exact lookup, reusable result | 43.37 ns/op, 0 B, 0 allocs | 46.59 ns/op, 0 B, 0 allocs | **1.07x faster** |
| Non-unique lookup, reusable IDs | 43.09 ns/op, 0 B, 0 allocs | 38.87 ns/op, 0 B, 0 allocs | 1.11x slower |
| Unique build, 10,000 rows | 1,037,623 ns/op, 732,590 B, 69 allocs | 1,012,599 ns/op, 873,879 B, 69 allocs | 2.5% slower, 16% less B/op |

The raw samples from `make benchmark-t219` are:

```text
hash unique lookup:       43.37, 37.86, 47.70 ns/op; 0 B/op; 0 allocs/op
functional value lookup:  46.08, 48.28, 46.59 ns/op; 0 B/op; 0 allocs/op
hash nonunique IDs:       43.78, 40.47, 43.09 ns/op; 0 B/op; 0 allocs/op
functional IDs:           36.44, 41.99, 38.87 ns/op; 0 B/op; 0 allocs/op
hash build:          947599, 1037623, 1300966 ns/op; 732626, 732590, 732589 B/op; 69 allocs/op
functional build:   1561650, 1012599, 1004124 ns/op; 873904, 873879, 873883 B/op; 69 allocs/op
```

The benchmark demonstrates where the structure earns its cost, not a blanket
replacement claim. HASH is most useful for unique or highly selective exact
predicates. Ordered and range workloads should use an ordered index, and
non-unique workloads should be benchmarked because compact posting traversal
can lose to an existing functional-index layout.
