# T-U18 Explicit Volatile Cache Engine

`hatDataStructure.VolatileEngine` is an importable, bounded, memory-only
key/value engine. It makes the durability choice explicit: values are never
written to a journal, snapshot, Pebble store, or temporary directory. Use it
for disposable cache state; use a durable data structure when recovery matters.

## Defaults

The zero value of `VolatileEngineOptions` selects these limits:

| Option | Default |
| --- | ---: |
| `MaxEntries` | 100,000 |
| `MaxBytes` | 64 MiB |
| `MaxKeyBytes` | 256 |
| `MaxValueBytes` | 16 MiB |
| `EvictionPolicy` | `VolatileEvictionReject` |

`MaxBytes` counts key bytes plus value bytes for admission. It is not a full
Go heap measurement, so leave operational headroom for map, list, mutex, and
allocator overhead.

## Example

```go
engine, err := hatDataStructure.NewVolatileEngine(hatDataStructure.VolatileEngineOptions{
    MaxEntries:     10000,
    MaxBytes:       64 << 20,
    EvictionPolicy: hatDataStructure.VolatileEvictionOldest,
})
if err != nil {
    return err
}

if err := engine.Set("session:42", []byte("payload"), 5*time.Minute); err != nil {
    return err
}

buffer, ok := engine.GetInto("session:42", buffer[:0])
if ok {
    use(buffer)
}
```

`Set` copies the input value. `Get` returns a copy, while `GetInto` reuses the
caller's buffer and returns a slice owned by that buffer. `Delete`, `Clear`,
`PurgeExpired`, `Len`, and `Stats` provide explicit maintenance and
observability operations.

Expiry is lazy: reads remove the requested expired key, writes sweep expired
entries before admission, and `PurgeExpired` or `Len` can be used for a full
sweep. `Now` is injectable for deterministic tests.

## Capacity behavior

The default reject policy preserves resident entries and returns
`ErrVolatileEngineCapacity` when a new or replacement value cannot fit. The
oldest policy removes insertion-oldest entries until the write fits and counts
those removals in `Stats().Evictions`. This is FIFO admission, not LRU: reads
do not reorder entries.

Invalid keys, oversized values, negative TTLs, invalid options, and nil engine
use return deterministic errors or a false result as documented by the Go API.

## Measured tradeoff

On Linux/amd64 with an AMD Ryzen 9 5950X, the focused command was:

```sh
make benchmark-t-u18-volatile-engine
```

Five samples with `-benchmem` produced these medians:

| Workload | Volatile engine | Plain map + mutex | Result |
| --- | ---: | ---: | --- |
| `GetInto`, 64-byte value | 55.23 ns/op, 0 B/op, 0 allocs/op | 10.78 ns/op, 0 B/op, 0 allocs/op | 5.12x slower |
| replacing `Set`, 64-byte value | 94.95 ns/op, 64 B/op, 1 alloc/op | 43.10 ns/op, 64 B/op, 1 alloc/op | 2.20x slower |

The plain map baseline omits TTL checks, byte/entry admission, cumulative
metrics, and eviction bookkeeping, so it is a lower-bound speed comparison,
not an apples-to-apples feature replacement. The engine's benefit is bounded
memory ownership and explicit volatile semantics without persistence setup;
the raw map remains the better choice when those controls are unnecessary.
