# CH-U30: Column-Aware Remote Read Policy

Hatrie now supports opt-in caching of projected remote-part columns or byte
ranges. This follows the ClickHouse remote-read idea of fetching only the
projected data needed by a query instead of retaining an entire immutable part.

## API

```go
part, err := hatStorage.NewRemotePartReference(
    "s3://bucket/parts/events-001",
    "parts/events-001.json",
    "sha256:part",
    64<<20,
)
column, err := hatStorage.NewRemotePartColumnReference(
    part,
    "customer_id",
    "sha256:customer-id-range",
    12<<20,
    256<<10,
)
cache, err := hatStorage.NewRemotePartCache(hatStorage.RemotePartCacheOptions{
    MaxBytes: 8 << 20,
})
values, err := cache.GetColumn(ctx, column, 2, func(
    ctx context.Context,
    reference hatStorage.RemotePartColumnReference,
) ([]byte, error) {
    return objectStore.ReadRange(ctx, reference.Part().ObjectURI(),
        reference.OffsetBytes(), reference.SizeBytes())
})
```

`GetColumn`, `AcquireColumn`, `PrefetchColumns`, and `InvalidateColumn` share
the existing cache's byte budget, entry limit, priority eviction, pinning,
single-flight loading, cancellation, and size validation. Whole-part methods
remain unchanged. A column key contains the parent object checksum, logical
column name, range offset, range size, and range checksum, so projected ranges
cannot collide with each other or with a whole-part entry.

The loader is deliberately injected. Hatrie does not assume an S3/GCS/Azure
client or silently perform network I/O. A caller that cannot issue range reads
can still use the API, but its loader will determine the actual bandwidth.

## Safety

- The feature is opt-in; the default cache remains disabled until a positive
  `MaxBytes` budget is supplied.
- Empty names/checksums, invalid parent references, NUL bytes, and offset/size
  overflow are rejected.
- A declared non-zero range size must match the loader result exactly.
- `PrefetchColumns` deduplicates identical identities and bounds concurrency.
- Returned bytes are copied into the cache and are immutable by contract.

## Tradeoff

The new key carries column identity and range metadata, and each projected
miss still allocates one cache entry plus one copied byte slice. That overhead
is worthwhile when the selected range is materially smaller than its parent
part. The caller should keep the existing whole-part path for queries that
consume most columns or repeatedly need the complete part.

The benchmark uses an in-memory loader, so its remote-byte result is a direct
bandwidth proxy rather than a cloud-provider latency claim.
