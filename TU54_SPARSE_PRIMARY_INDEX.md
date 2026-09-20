# T-U54 Sparse Primary Index

## Status

Adopted as the opt-in generic `hatDataStructure.SparsePrimaryIndex[K]`. It
keeps one sorted key anchor per configured row stride and returns the bounded
row window that may contain a lower-bound match. Existing indexes and query
paths are unchanged until a caller explicitly constructs it.

## Inspiration and Boundary

ClickHouse MergeTree-style primary indexes are sparse marks rather than a
pointer for every row. This reduces index memory and lets the engine scan a
small granule after locating a mark. Materialize arrangements and Tarantool
ordered-space scans motivate the same separation between a compact navigation
structure and caller-owned row storage.

The index stores no row values and does not perform the final predicate scan.
For duplicate keys, callers must continue through subsequent windows while the
row predicate still matches. This makes the behavior explicit instead of
silently dropping duplicate rows.

## Example

```go
index, err := hatDataStructure.NewSparsePrimaryIndex(
	64,
	func(left, right uint64) bool { return left < right },
)
if err != nil {
	return err
}
if err := index.Build(sortedKeys); err != nil {
	return err
}
window, ok := index.Window(queryKey)
if ok {
	for _, row := range rows[window.Start:window.End] {
		// Verify the actual key/predicate in caller-owned row storage.
		_ = row
	}
}
```

`Build` accepts duplicates, validates nondecreasing input, and replaces the
anchors only after validation succeeds. A rejected rebuild therefore leaves
the previous index usable. The stride is bounded to avoid accidental giant
scan windows or integer overflow in anchor allocation.

## Tradeoff Measurement

Five-sample median on AMD Ryzen 9 5950X, Linux amd64, 1,048,576 sorted
`uint64` keys and stride 64:

| Operation | Full-key baseline | Sparse index | Difference |
| --- | ---: | ---: | ---: |
| Build/copy | 818,516 ns/op, 8,388,620 B/op | 1,687,023 ns/op, 262,144 B/op | 2.06x slower, 32.0x less memory |
| Lookup stage | 90.67 ns/op exact lower-bound row | 77.40 ns/op block locate | 1.17x faster stage |

The build comparison is intentionally labeled: the baseline copies every key,
while the sparse build also validates ordering and creates anchors. The lookup
comparison is not an end-to-end equality lookup: the full index returns an
exact row position, while the sparse index returns a block and requires a
caller scan of at most the configured stride. The memory reduction is the
primary benefit; choose a stride according to scan cost and cache locality.
