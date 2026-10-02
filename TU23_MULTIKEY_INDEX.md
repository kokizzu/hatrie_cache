# T-U23 Multikey Indexes

This feature provides two opt-in APIs in `hatDataStructure`:

- `MultiKeyIndex[T, K]` derives a bounded set of comparable keys from each
  typed record.
- `StringMultikeyIndex` is a compact string-only adapter for tuple fields such
  as tags, labels, or alternate names.

Each distinct key maps to sorted stable IDs. Repeated elements in one record
are indexed once. Reverse key ownership makes updates and deletes exact.

## Generic API

```go
type Document struct {
	ID   uint64
	Tags []string
}

index, err := hatDataStructure.NewMultiKeyIndex(
	func(document Document) []string { return document.Tags },
	hatDataStructure.MultiKeyIndexOptions{
		Capacity:        10000,
		MaxKeysPerEntry: 8,
		MaxItems:        10000,
	},
)
if err != nil {
	return err
}
if err := index.Upsert(document.ID, document); err != nil {
	return err
}
ids := index.LookupIDs("database", nil)
```

`LookupIDsInto` and `LookupInto` reset and reuse caller-owned destinations.
`Contains`, `ContainsID`, `LookupOne`, `Len`, `DistinctKeys`, `Delete`, and
`Clear` cover the common exact-match and lifecycle operations.

## String adapter

```go
tags := hatDataStructure.NewStringMultikeyIndex(
	hatDataStructure.StringMultikeyIndexOptions{
		MaxKeysPerItem: 8,
		MaxItems:       10000,
	},
)
if err := tags.Set(42, []string{"go", "database", "go"}); err != nil {
	return err
}
ids := tags.Lookup("database", nil) // []uint64{42}
```

The first two string keys are stored inline. Only larger records allocate an
overflow slice. Calling `Set` with an empty slice removes the item, matching
the existing string-index contract.

## Bounds and correctness

- Generic `MaxKeysPerEntry: 0` selects a default of 64 and a hard maximum of
  4096.
- The compatibility `StringMultikeyIndexOptions.MaxKeysPerItem` keeps its
  original semantics: zero or a negative value means unlimited; positive
  values are enforced as supplied.
- `MaxItems: 0` means no item admission bound; a positive value rejects new
  IDs after the bound is reached.
- Oversized input and item-limit failures happen before mutation, preserving
  the old postings and preventing partial updates.
- Empty key sets are valid for the generic index and still count as an item;
  the string adapter treats empty input as deletion for compatibility.
- Lookups are stable by ascending ID and are safe concurrently with `Set`,
  `Delete`, and `Lookup`.
- Existing `HashIndex`, `FunctionalIndex`, and SQL paths are unchanged.

## Measurements

Five benchmark samples on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | Baseline | Multikey index | Improvement | Memory/allocations |
| --- | ---: | ---: | ---: | --- |
| Generic three-key update | 217.4 ns/op | 81.14 ns/op | 2.68x faster | 0 B/op, 0 allocs/op for both |
| Indexed lookup, 100k rows | 8,423 ns/op map-of-sets | 88.57 ns/op | 95.1x faster | 0 B/op, 0 allocs/op |
| Indexed lookup versus linear scan | 171,563 ns/op | 88.57 ns/op | 1,938x faster | indexed path has 0 B/op |
| Bounded build, 10k rows | 2,210,890 ns/op | 2,204,260 ns/op | 1.00x | 2,056,904 vs 1,800,456 B/op; 11,616 vs 1,300 allocs |

The bounded build result uses `MaxItems: 10000`, so the reverse item map is
pre-sized and the postings map is allowed to size itself from distinct keys.
This is the recommended ingestion configuration when an upper bound is known.

The unbounded default build is a different tradeoff: it measured about
3.57 ms, 3.11 MB, and 1,348 allocations for the compact index versus about
2.03 ms, 2.06 MB, and 11,616 allocations for the pre-sized reverse-map
baseline. The default keeps the existing unbounded API behavior and avoids an
implicit large reservation; callers that know their cardinality should set
`MaxItems`.

Raw benchmark commands and samples are recorded in [BENCHMARK.md](BENCHMARK.md#t-u23-multikey-indexes).

## Verification

```sh
make test-tu23
make test-tu23-package
make race-tu23-package
make vet-tu23
make benchmark-tu23-comparison
make benchmark-tu23-existing-lookup
make benchmark-tu23-bounded-build
make verify-tu23
```
