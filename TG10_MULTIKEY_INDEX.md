# T-G10 Multikey Secondary Index

This is a Tarantool-inspired multikey secondary index for string-valued array
membership. One item ID can be indexed under several keys, and replacing the
item atomically removes stale postings before installing the new set.

## API

```go
index := hatDataStructure.NewStringMultikeyIndex(
    hatDataStructure.StringMultikeyIndexOptions{
        MaxKeysPerItem: 8,
        MaxItems:       1_000_000,
    },
)

err := index.Set(42, []string{"admin", "active"})
ids := index.Lookup("active", reusableBuffer)
ok := index.Contains("admin", 42)
index.Delete(42)
```

`Set` sorts and deduplicates keys before mutation. A rejected key or item limit
leaves the previous index state unchanged. `Lookup` returns sorted IDs and
appends into the caller-provided buffer, allowing repeated reads without an
allocation. `Len` counts indexed items and `KeyCount` counts nonempty posting
lists.

The implementation uses compact sorted `[]uint64` posting lists plus a reverse
`item ID -> keys` map. Sorted slices give deterministic results and fast
contiguous copying; the reverse map makes update/delete correctness possible.
The index is protected by an `RWMutex`.

## Benchmark

Linux amd64, AMD Ryzen 9 5950X, five samples for lookup and three for build,
100,000 items for lookup, two keys per item, and reusable result buffers.

| Operation | Median ns/op | B/op | allocs/op | Relative result |
|---|---:|---:|---:|---:|
| Sorted posting lookup | 81.9 | 0 | 0 | baseline |
| Map-of-sets lookup | 8,933 | 0 | 0 | sorted is 109x faster |
| Linear source scan | 177,134 | 1 | 0 | sorted is 2,162x faster |

For building 10,000 items with two keys, the sorted index used `2,115,912`
bytes and `11,215` allocations at a median `2,635,554 ns/op`. A map-of-sets
with the same reverse map used `2,056,913` bytes and `11,616` allocations at
`2,318,312 ns/op`: sorted postings are about `2.9%` more memory, `3.5%` fewer
allocations, and `13.7%` slower to build. That is an intentional read-heavy
tradeoff, not a claim that it is cheaper for write-heavy workloads.

Run verification with:

```text
make test-multikey-index
make race-multikey-index
make vet-multikey-index
make benchmark-multikey-index
make benchmark-multikey-index-build
```
