# Remote-part cache

`hatStorage.RemotePartCache` is an opt-in bounded cache for immutable parts
loaded from S3, GCS, Azure, HTTP, or HTTPS references. It is useful when a
query repeatedly reads the same remote part and the caller wants to trade a
bounded amount of memory for lower remote-read latency.

```go
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
	MaxBytes: 256 << 20,
})
if err != nil {
	return err
}

part, err := cache.Get(ctx, reference, 10, loader)
if err != nil {
	return err
}
_ = part // Treat returned bytes as immutable.
```

`MaxBytes` must be positive, so construction is disabled unless the caller
chooses a memory budget. `MaxEntries` defaults to 1,024 when zero. A loaded
part must match its declared `SizeBytes`; failed loads are never retained.

Use `Acquire` when a read must protect a cached entry from eviction:

```go
lease, err := cache.Acquire(ctx, reference, 10, loader)
if err != nil {
	return err
}
defer lease.Release()
consume(lease.Bytes())
```

Higher priorities survive ahead of lower priorities. Equal priorities use
least-recently-used eviction. Pinned entries are skipped; if every possible
victim is pinned, the loaded bytes are returned without retaining a cache
entry. Concurrent misses for the same reference share one loader call.

`Get` is the low-overhead read path and returns zero-copy cached bytes. The
cache owns a separate copy created on a successful miss, so a loader may reuse
its input buffer after returning. `Acquire` adds a small handle allocation per
read in exchange for explicit pinning. `Stats` reports hits, misses, loads,
evictions, uncached loads, entries, and retained bytes; `Invalidate` removes a
cached reference without invalidating already-issued handles.

## Measured tradeoff

The benchmark uses one repeated 64 KiB part and five samples per path on the
same host. The direct-loader baseline copies the part for every read. Warm
`Get` is about 145.6x faster with no per-read allocation; pinned
`Acquire`/`Release` is about 94.9x faster and allocates a 64-byte handle per
read. The cost is bounded retained memory: the warmed entry occupies 65,536
part bytes plus cache metadata, up to the configured `MaxBytes` budget.

See the complete raw samples in [BENCHMARK.md](BENCHMARK.md#ch-008-remote-part-cache).
