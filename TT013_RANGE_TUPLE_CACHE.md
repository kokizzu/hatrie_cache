# TT-013 Range Tuple Cache

This adds an importable, opt-in `hatDataStructure.RangeTupleCache[K, V]`
inspired by Tarantool Vinyl's range-aware caching. It retains exact results for
`(start, end)` ranges, uses a bounded LRU, and associates every entry with a
caller-owned source version.

## API and invariants

```go
cache, err := hatDataStructure.NewRangeTupleCache[int, Row](128)
if err != nil {
    return err
}
if err := cache.Put(lower, upper, sourceVersion, rows); err != nil {
    return err
}
rows, hit := cache.Get(lower, upper, sourceVersion)
```

- `K` is `comparable`, so range keys use a map lookup without serializing or
  allocating a key.
- `Put` clones its input slice, allowing callers to reuse their scan buffer.
- `Get` is zero-allocation on a hit. Its returned slice is cache-owned and must
  be treated as read-only.
- A version mismatch is a miss and removes the stale range.
- `capacity` bounds the number of retained ranges; `Stats` reports hits,
  misses, evictions, and current entries.
- The cache is disabled by omission: no existing index or storage path creates
  one automatically.

## Benchmark

The workload scans 16 contiguous rows from a 4,096-entry `OrderedIndex` and
repeats the exact same range. Five samples were run on an AMD Ryzen 9 5950X.

| Path | Median ns/op | B/op | allocs/op | Relative latency |
| --- | ---: | ---: | ---: | ---: |
| Existing `OrderedIndex.Range` scan | 113.5 | 0 | 0 | 1.00x |
| `RangeTupleCache.Get` hit | 23.98 | 0 | 0 | 0.21x |

The hot read is therefore approximately **4.73x faster** with no additional
per-hit allocation. Reproduce the measurements with:

```text
make benchmark-tt013-range-cache-baseline
make benchmark-tt013-range-cache
```

## Tradeoff

Admission copies each result slice, so a cached range retains its values plus
one small LRU entry and map entry. This is intentional: it prevents the caller's
reused scan buffer from corrupting cached results. Memory is bounded by the
configured entry capacity, and version replacement prevents stale ranges from
accumulating under a single key. Cache admission, invalidation, and source
version management remain caller-owned because automatically caching mutable
storage would risk serving stale rows.

Package tests, race detection, and vet are run by
`make verify-tt013-range-cache`.
