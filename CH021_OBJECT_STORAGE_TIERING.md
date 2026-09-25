# CH-021 Object-Storage Tiering

CH-021 adopts the ClickHouse-style hot/local and cold/object-storage read
boundary as an opt-in `hatStorage.StorageTierReader`.

## What is implemented

- `StorageTierReadPart` describes one immutable part and its published location.
- `StorageTierLocal` reads a relative path below a configured local root.
- `StorageTierObject` reads a `RemotePartReference` through the existing bounded
  `RemotePartCache` and injected `RemotePartCacheLoader`.
- Both locations use the same `Read(context.Context, StorageTierReadPart)` API.
- Local paths reject absolute paths, NUL bytes, and lexical traversal outside
  the configured root.
- Object reads inherit the cache's byte/entry budget, priority, deduplication,
  and immutable-byte contract.
- The default storage and read paths are unchanged; constructing a reader is
  required to enable this behavior.

## Example

```go
reader, err := hatStorage.NewStorageTierReader(hatStorage.StorageTierReaderOptions{
    LocalRoot:    "/var/lib/hatrie/parts",
    RemoteCache:  cache,
    RemoteLoader: loadRemotePart,
})
if err != nil {
    return err
}

data, err := reader.Read(ctx, hatStorage.StorageTierReadPart{
    Key:       "part-2026-09-25",
    Tier:      hatStorage.StorageTierObject,
    Remote:    remoteReference,
    Priority:  10,
})
```

The loader owns authentication, transport, retries, and remote checksum
verification. Tier movement, publication, deletion, and metadata durability
remain caller-owned so an incomplete move cannot make a part disappear.

## Benchmark tradeoff

Measured with a 64 KiB part on the repository's AMD Ryzen 9 5950X runner,
`-benchmem -count=5`; the table reports the median of the five runs:

| Path | Before | After | Relative cost | Allocation change |
| --- | ---: | ---: | ---: | ---: |
| Direct local `os.ReadFile` | 21.57 us/op, 74,104 B/op, 5 allocs | Tiered local read: 23.10 us/op, 74,168 B/op, 6 allocs | 1.07x slower | +64 B, +1 alloc |
| Existing remote cache hit | 63.14 ns/op, 0 B/op, 0 allocs | Tiered object cache hit: 75.12 ns/op, 0 B/op, 0 allocs | 1.19x slower | unchanged |

This is a small dispatch/metadata cost for a new storage capability, not a
claim that the wrapper is faster than its underlying read. The remote cache
still avoids repeated object loads and part copies on hits. The per-read path
uses lexical root validation; the local root and part metadata are expected to
be operator-controlled, matching the existing caller-owned filesystem trust
boundary.

## Verification

```text
make test-ch021-tiered-read
make race-ch021-tiered-read
make vet-ch021-tiered-read
make benchmark-ch021-tiered-read
```
