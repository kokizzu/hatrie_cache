# C243 Remote-Part Read-Through Cache

Status: implemented in `hatStorage.RemotePartCache`.

The cache treats a remote part as immutable and keys each entry by the
normalized object URI, checksum identity, and declared byte size. It provides
bounded byte and entry budgets, single-flight loading, priority-aware eviction,
pinned handles, explicit invalidation, and bounded prefetch. The cache is
opt-in: construction requires a positive `MaxBytes` budget.

## Integrity Decision

The checksum is an immutable cache identity, not a second payload-validation
pass. A candidate that computed SHA-256 for every canonical miss was measured
before delivery and rejected: cache hits stayed at roughly 64-67 ns/op, while
64 KiB misses increased from roughly 11.5-12.6 us/op to 43.4-45.2 us/op, with
no reduction in bytes or allocations. Remote loaders that need content
verification should validate the object before returning it to the cache.

## Verification

The focused regression test is `TestRemotePartCacheC243UsesImmutableChecksumKey`.
The isolated benchmark compares the repository `HEAD` implementation with the
working tree through `make benchmark-c243-isolated` and records hit/miss CPU,
memory, and allocation behavior in `BENCHMARK.md`.
