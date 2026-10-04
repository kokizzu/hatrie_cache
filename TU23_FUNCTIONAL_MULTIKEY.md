# T-U23 Typed Functional Multikey Index

T-U23 adds `hatDataStructure.FunctionalMultikeyIndex[T, K]`. It accepts a
typed row extractor returning a slice of comparable keys, deduplicates the
keys, and maintains compact sorted row-ID postings for every key. This turns
the existing manual `StringMultikeyIndex.Set(id, extractor(row))` pattern into
a maintained typed index that also supports integers and other comparable
key types.

```go
index, err := hatDataStructure.NewFunctionalMultikeyIndex(
	func(row User) []string { return row.Tags },
	hatDataStructure.FunctionalMultikeyIndexOptions{
		MaxKeysPerItem: 8,
		MaxItems:       100_000,
	},
)
if err != nil {
	panic(err)
}

if err := index.Upsert(userID, user); err != nil {
	panic(err)
}
matches := index.Lookup("analytics", nil)
```

The same API works with `K=int`, date ordinals, or another comparable typed
key. `Lookup` accepts a reusable destination, and `Delete`, `Contains`, `Len`,
and `KeyCount` mirror the existing multikey index behavior. Bounds are checked
before mutation, so rejected key/item-limit updates leave prior postings intact.
The feature is opt-in and does not change existing SQL or index defaults.

For small arrays, deduplication uses a bounded linear scan to avoid a helper
map allocation. Larger arrays use a temporary map for linear-time deduplication.
Keys retain first-seen order; callers that reorder the same set will incur a
replacement update, but lookup results remain identical and row IDs remain
sorted.

## Measurement

The baseline is the existing manual `StringMultikeyIndex.Set` path. The
candidate extracts the same `[]string` through `FunctionalMultikeyIndex`.
Five `-count=5` samples were run on Linux/amd64, AMD Ryzen 9 5950X, with four
keys per row and 10,000 prepared rows.

| Workload | Baseline median | Candidate median | Improvement |
| --- | ---: | ---: | ---: |
| Steady upsert | 127.3 ns/op, 64 B/op, 1 alloc/op | 91.07 ns/op, 64 B/op, 1 alloc/op | 1.40x faster |
| Build 10,000 rows | 4,646,924 ns/op, 2,999,132 B/op, 18,404 allocs/op | 4,160,671 ns/op, 2,999,133 B/op, 18,404 allocs/op | 1.12x faster, allocation-neutral |

Run the permanent benchmark with `make benchmark-tu23-functional-multikey`.
Raw samples are recorded in `BENCHMARK.md`.
