# T-U18 Explicit Volatile Cache Engine

`hatCache.NewVolatileEngine` is an opt-in, memory-only byte cache backed by the
existing HAT-trie. It has no filesystem path, journal, checkpoint, persistent
store, or backup surface, so durability intent is explicit in the type and
cannot be confused with `PersistentStore`.

## Usage

At least one bound is required. A zero bound disables that dimension.

```go
engine, err := hatCache.NewVolatileEngine(hatCache.VolatileEngineOptions{
	MaxBytes:   256 << 20,
	MaxEntries: 100000,
})
if err != nil {
	return err
}
defer engine.Close()

if err := engine.SetBytes("session:42", []byte("payload"), 15*time.Minute); err != nil {
	return err
}
value, present, err := engine.GetBytes("session:42")
```

The engine uses FIFO eviction when either configured bound is reached. Updating
an existing key preserves its FIFO position. TTL expiry is generation-safe,
and `Vacuum` can remove expired values explicitly. `Stats` reports entries,
logical resident bytes, evictions, expirations, and hits/misses.

`ResidentBytes` is an accounting estimate: `64 + len(key) + len(value)` per
entry. It is a bounded admission budget, not a process RSS reading. Oversized
entries are rejected before eviction or mutation. Existing default HAT-trie
and persistent-store behavior is unchanged because this constructor is opt-in.

## Benchmark

Commands:

```text
make baseline-tu18
make benchmark-tu18
make test-tu18
make race-tu18
make vet-tu18
```

Five `-benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X, using one byte
key/value set followed by get per operation:

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Relative |
| --- | --- | ---: | ---: | ---: | --- |
| Direct HAT-trie byte path, baseline | 242.9; 244.1; 243.1; 243.1; 244.9 | 243.1 | 64 | 2 | baseline |
| `VolatileEngine`, after | 338.1; 338.6; 335.2; 341.2; 337.8 | 338.1 | 64 | 2 | 1.39x CPU; +95.0 ns |

The opt-in contract adds about 39% CPU for admission, metadata, and lifecycle
accounting without increasing benchmark allocations or per-operation allocated
bytes. The tradeoff is bounded volatile memory and an explicit non-durable
operator contract, not a claim that the wrapper is faster than direct HAT-trie
access.
