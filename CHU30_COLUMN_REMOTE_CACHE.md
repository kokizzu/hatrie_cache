# CH-U30 Column-Aware Remote-Part Cache

`RemotePartCache` remains disabled unless the caller supplies a positive
`MaxBytes` budget. When enabled, the existing whole-part APIs keep their
original behavior. The opt-in column APIs cache a remote column independently
from other columns in the same immutable part.

## API

```go
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
	MaxBytes:   64 << 20,
	MaxEntries: 1024,
})
if err != nil {
	return err
}

payload, err := cache.GetColumn(ctx, reference, "customer_id", 1,
	func(ctx context.Context, reference hatStorage.RemotePartReference, column string) ([]byte, error) {
		return readRemoteColumn(ctx, reference, column)
	})
```

The related operations are:

- `GetColumn` reads without pinning the entry.
- `AcquireColumn` returns a handle that pins the entry until `Release`.
- `PrefetchColumns` performs bounded concurrent read-ahead and deduplicates
  `(part, column)` requests.
- `InvalidateColumn` removes one column without invalidating other columns or
  the whole-part entry.

The cache identity is the remote-part reference plus the normalized column
name. Whole-part and column entries share `MaxBytes`, `MaxEntries`, priority
eviction, pinning, hit/miss/load counters, and uncached accounting. A column
payload is not compared with the whole-part size in `RemotePartReference`; a
column loader is responsible for validating the column's own encoding and
integrity.

Column names must be non-empty after trimming, contain no NUL byte, and be at
most 256 bytes. Context cancellation and loader errors do not publish partial
entries. Concurrent requests for the same `(part, column)` use single-flight
loading.

## Default And Compatibility

No existing caller is changed automatically. `Get`, `Acquire`, `Prefetch`, and
`Invalidate` remain whole-part operations, and `MaxBytes: 0` still disables the
cache. A caller should select the column APIs only when the remote source can
return independently validated column payloads and the workload usually reads
a subset of each part.

## Measurement

The benchmark uses five `-count=5` samples on Linux amd64 with an AMD Ryzen 9
5950X. The existing whole-part hot paths were measured on the clean CH-U55
base and again after CH-U30. The column hit benchmark uses a warmed cache and
reports zero bytes and zero allocations per lookup.

| Operation | Clean base median | CH-U30 median | Result |
| --- | ---: | ---: | --- |
| Whole-part cached `Get` | 66.98 ns/op, 0 B, 0 allocs | 66.76 ns/op, 0 B, 0 allocs | 1.00x; no regression |
| Whole-part pinned `Acquire`/`Release` | 116.4 ns/op, 64 B, 1 alloc | 114.1 ns/op, 64 B, 1 alloc | 1.02x faster; same memory |
| Column cached `GetColumn` | N/A | 95.50 ns/op, 0 B, 0 allocs | New opt-in path |
| Column prefetch, zero-latency, bounded 2 | N/A | 23,669 ns/op, 5,355-5,357 B, 53 allocs | New opt-in path |
| Column prefetch, 100 us loader latency, bounded 2 | N/A | 4,297,195 ns/op, 5,560-5,577 B, 55-56 allocs | Loader latency dominates |

The column path adds a second bounded map and pays column-key hashing only for
column operations. Existing whole-part callers do not pay column payload
allocations or loader dispatch. See the raw samples in
[`BENCHMARK.md`](BENCHMARK.md#ch-u30-column-aware-remote-part-cache).
