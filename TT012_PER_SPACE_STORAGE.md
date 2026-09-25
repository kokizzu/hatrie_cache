# TT-012 Per-Space Storage Engines

Hatrie Cache now provides an opt-in registry for applications that keep more
than one logical space. Each named space can select its own persistent engine,
record format, and directory while sharing one lifecycle owner.

## Example

```go
spaces, err := hatCache.OpenPersistentSpaceStoreSet([]hatCache.PersistentSpaceStoreConfig{
	{
		Name:    "hot",
		Path:    "/var/lib/hatrie/hot",
		Backend: hatCache.StorageBackendPebble,
		Format:  hatCache.StorageFormatBinary,
	},
	{
		Name:    "archive",
		Path:    "/var/lib/hatrie/archive",
		Backend: hatCache.StorageBackendLevelDB,
		Format:  hatCache.StorageFormatBinary,
	},
})
if err != nil {
	return err
}
defer spaces.Close()

hot, ok := spaces.Store("hot")
if !ok {
	return errors.New("hot space is not configured")
}
if err := hot.Save(hotTrie); err != nil {
	return err
}
```

`Backend` and `Format` may be left empty to use the existing auto/backend and
default-format behavior. A durable backend marker remains authoritative on
reopen, so changing a configured engine against an existing directory fails
instead of silently opening it with the wrong engine.

## Guarantees

- Names are trimmed and must be unique.
- Cleaned absolute paths must be unique, preventing two handles from owning
  the same directory through equivalent path spellings.
- A failed partial open closes every store opened before the failure.
- `Names` is deterministic and sorted.
- `Store` is O(1), allocation-free, and safe for concurrent lookups.
- `Close` is idempotent; lookups after close return `(nil, false)`.
- Each store retains the existing backend marker, persistence, backup, and
  restore behavior. The registry does not add cross-space transactions.

## Tradeoffs

The registry owns one engine handle per space. That gives independent Pebble or
LevelDB policy and failure boundaries, but increases file descriptors, cache
memory, background work, and backup coordination as the number of spaces grows.
There is also no atomic commit spanning two spaces; applications needing that
property must continue using one store or an external journal/transaction
protocol.

Lookup benchmark medians on Linux amd64, AMD Ryzen 9 5950X, five samples:

| Lookup | ns/op | B/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| `PersistentSpaceStoreSet.Store` | 14.48 | 0 | 0 | 1.81x the direct-map baseline |
| Direct map lookup | 7.99 | 0 | 0 | 1.00x baseline |

The registry's `6.49 ns` synchronization cost is intentional. Resolve a space
once per request/worker and retain the `PersistentStore` for tight loops.

## Verification

```text
make format-tt012-space-store
make test-tt012-space-store
make race-tt012-space-store
make vet-tt012-space-store
make benchmark-tt012-space-store
```

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#tt-012-per-space-storage-engines).
