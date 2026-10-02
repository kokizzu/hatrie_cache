# T-U23 Automatic Multikey Index

`hatDataStructure.MultikeyIndex[K]` provides a bounded typed multikey posting
index for any comparable key, including integers, fixed comparable structs,
and other tuple-like values. `TypedMultikeyIndex[T, K]` adds the automatic
tuple/array-facing layer: the caller supplies an extractor and `Upsert`
maintains all postings whenever an item changes.

## Example

```go
type Product struct {
	Tags []string
}

tags, err := hatDataStructure.NewTypedMultikeyIndex(
	func(product Product) []string { return product.Tags },
	hatDataStructure.MultikeyIndexOptions{
		MaxKeysPerItem: 16,
		MaxItems:       100_000,
	},
)
if err != nil {
	return err
}
if err := tags.Upsert(42, Product{Tags: []string{"red", "large", "red"}}); err != nil {
	return err
}
ids := tags.Lookup("red", nil)
```

For already extracted numeric or composite keys, use
`NewMultikeyIndex[Key](options)` and call `Set`. Both APIs deduplicate keys,
keep posting IDs in stable ascending order, remove old postings on replacement,
and make failed bounds checks atomic.

## Contract and limits

- `MaxKeysPerItem` bounds the raw extracted slice before normalization; a
  nonpositive value is unlimited.
- `MaxItems` bounds distinct indexed IDs; updating an existing ID remains
  allowed even when the limit is full.
- An empty key slice removes the item from the index.
- Comparable keys remain typed and are not converted to strings, avoiding
  serialization and string-key allocations for integer/date-like or fixed
  composite values.
- `Lookup(key, dst)` reuses caller-owned result storage. Posting IDs are kept
  in the compact shared `u64PostingList` representation.
- The index is process-local. The caller owns row durability, replication,
  schema extraction, and recovery.
- Extractors should be deterministic and should return a stable view of the
  item keys for the duration of `Upsert`.

The existing `StringMultikeyIndex` API is unchanged. This feature is opt-in;
it does not automatically add indexes to existing tables or alter query
planning.

## Verification

Focused, race, full-package, and vet verification cover scalar and composite
keys, extractor-backed updates, deduplication, replacement cleanup, bounds
atomicity, reusable lookup destinations, clear/delete behavior, and concurrent
read/write access.

## Benchmark interpretation

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X, using
`-benchtime=1s -benchmem`, compare the typed index with an equivalent manual
map plus slice-posting implementation for 1,024 resident IDs and four keys per
item:

| Workload | Manual baseline | Typed index | Result |
|---|---:|---:|---|
| Same-key `Set` | 39.61 ns/op, 32 B/op, 1 alloc | 24.57 ns/op, 0 B/op, 0 alloc | 1.61x faster; allocation-free |
| Changed-key `Set` | 1,069 ns/op, 130 B/op, 1 alloc | 311.7 ns/op, 130 B/op, 1 alloc | 3.43x faster; same heap |
| Lookup with reusable dst | 65.91 ns/op, 0 B/op, 0 alloc | 72.24 ns/op, 0 B/op, 0 alloc | 1.10x slower; same heap |

The lookup slowdown is the measured cost of the compact posting abstraction;
the index trades a small read CPU cost for typed keys and the much lower
changed-key maintenance cost. Raw samples are preserved in the benchmark
history for this feature:

| Workload | Manual baseline samples (ns/op) | Typed index samples (ns/op) |
|---|---|---|
| Same-key `Set` | 38.63, 39.61, 42.36, 40.09, 39.57 | 24.35, 24.52, 24.89, 24.89, 24.57 |
| Changed-key `Set` | 1,068, 1,140, 1,102, 1,069, 1,068 | 307.4, 311.7, 313.9, 307.8, 313.7 |
| Lookup with reusable dst | 65.52, 65.16, 65.91, 71.05, 70.35 | 77.60, 68.95, 70.83, 72.24, 74.27 |
