# CH-020 Zero-Copy Remote-Part Cache Admission

`hatStorage.RemotePartCache` normally copies the byte slice returned by a
loader. That default is safe for loaders that reuse or mutate their read
buffer. Callers that can transfer ownership of an immutable buffer can use the
additive zero-copy APIs:

```go
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
	MaxBytes:        64 << 20,
	VerifyChecksums: true,
})
part, err := cache.GetOwned(ctx, reference, 0, func(context.Context, hatStorage.RemotePartReference) ([]byte, error) {
	return immutableBuffer, nil
})
```

`GetOwned` and `AcquireOwned` accept `RemotePartCacheOwnedLoader`. The loader
must not retain, mutate, or reuse its returned slice after the callback
returns. The cache may retain and return that exact backing buffer until
eviction; callers must treat returned bytes as immutable. `AcquireOwned`
provides the same ownership transfer with an explicit pin released by
`RemotePartHandle.Release`.

The existing `Get`, `Acquire`, `RemotePartCacheLoader`, and prefetch APIs keep
their copy-on-admission behavior. No configuration flag changes the default.
Checksum and declared-size validation still run before admission when
`VerifyChecksums` is enabled, including for owned loaders. If the cache cannot
retain an owned part because of its bounds, the transferred buffer is returned
without an additional copy and is not retained.

## Measurements

The focused benchmark uses a 64 KiB payload on Linux/amd64 with an AMD Ryzen 9
5950X and five samples per case. The post-feature copy path is the matched
control for the same cache implementation.

| Workload | Median ns/op | Median B/op | Median allocs/op | Improvement / cost |
| --- | ---: | ---: | ---: | --- |
| Pre-feature copy cold miss | 8,696 | 65,808 | 4 | baseline |
| Post-feature copy cold miss | 7,799 | 65,808 | 4 | compatibility control |
| `GetOwned` cold miss | 332.0 | 272 | 3 | 23.5x CPU; 241.9x lower B/op; 1.33x fewer allocs |
| `AcquireOwned` cold miss, verification off | 389.8 | 336 | 4 | 20.0x CPU; 195.9x lower B/op; same alloc count |
| `AcquireOwned` cold miss, SHA-256 verification on | 30,739 | 336 | 4 | integrity cost; no copy allocation |

The large gain is from eliminating the 64 KiB cache copy. Ownership transfer is
intentionally opt-in because violating the immutable-buffer contract creates a
data-race or cache-corruption risk. The compatibility APIs retain their
previous behavior.

### Raw baseline output

```text
BenchmarkCH020RemotePartCacheCopyColdMissBaseline-32 157575 7646 ns/op 8570.81 MB/s 65808 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissBaseline-32 135912 8273 ns/op 7921.76 MB/s 65808 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissBaseline-32 140864 9183 ns/op 7136.43 MB/s 65808 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissBaseline-32 126193 8696 ns/op 7536.66 MB/s 65809 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissBaseline-32 142610 8904 ns/op 7360.32 MB/s 65809 B/op 4 allocs/op
```

### Raw post-feature output

```text
BenchmarkCH020RemotePartCacheCopyColdMissAfter-32 146920 7799 ns/op 8403.17 MB/s 65808 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissAfter-32 168973 7175 ns/op 9134.18 MB/s 65808 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissAfter-32 148792 8082 ns/op 8109.09 MB/s 65809 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissAfter-32 171432 7239 ns/op 9053.65 MB/s 65810 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheCopyColdMissAfter-32 119902 8690 ns/op 7541.76 MB/s 65809 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedColdMiss-32 3745347 325.4 ns/op 201382.57 MB/s 272 B/op 3 allocs/op
BenchmarkCH020RemotePartCacheOwnedColdMiss-32 3754467 322.2 ns/op 203412.91 MB/s 272 B/op 3 allocs/op
BenchmarkCH020RemotePartCacheOwnedColdMiss-32 3741471 332.0 ns/op 197410.01 MB/s 272 B/op 3 allocs/op
BenchmarkCH020RemotePartCacheOwnedColdMiss-32 3525028 338.8 ns/op 193443.17 MB/s 272 B/op 3 allocs/op
BenchmarkCH020RemotePartCacheOwnedColdMiss-32 3527263 347.0 ns/op 188848.99 MB/s 272 B/op 3 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissNoVerification-32 2945000 389.8 ns/op 168134.93 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissNoVerification-32 3098611 395.0 ns/op 165920.40 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissNoVerification-32 2862544 402.7 ns/op 162742.62 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissNoVerification-32 3078502 386.2 ns/op 169674.30 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissNoVerification-32 3141662 386.7 ns/op 169455.31 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissWithVerification-32 37611 31608 ns/op 2073.37 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissWithVerification-32 35430 30367 ns/op 2158.14 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissWithVerification-32 38284 30809 ns/op 2127.15 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissWithVerification-32 39188 30739 ns/op 2132.00 MB/s 336 B/op 4 allocs/op
BenchmarkCH020RemotePartCacheOwnedAcquireColdMissWithVerification-32 33619 30612 ns/op 2140.86 MB/s 336 B/op 4 allocs/op
```
