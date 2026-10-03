# TT-018 / CH-015 Frequency Admission Cache

`hatDataStructure.FrequencyAdmissionCache[K, V]` is an opt-in, non-thread-safe
cache for hot ranges, page indexes, decoded blocks, or other bounded derived
state. It uses a fixed-capacity recency list plus a packed four-bit frequency
sketch. A new key is admitted only when its estimated frequency is greater than
the least-recently-used resident key, which prevents a one-pass scan from
evicting repeatedly reused entries.

```go
cache, err := hatDataStructure.NewFrequencyAdmissionCache[string, []byte](
	hatDataStructure.FrequencyAdmissionCacheOptions[string]{
		Capacity: 4096,
		Hash:     func(key string) uint64 { return xxhash.Sum64String(key) },
	},
)
if err != nil {
	return err
}

if value, ok := cache.Get(key); ok {
	return value
}
if value, err := load(key); err == nil {
	if !cache.Set(key, value) {
		// The value was intentionally rejected as colder than the resident tail.
	}
}
```

`Hash` must remain stable for the lifetime of the cache. `Get` misses are
counted by the sketch so callers can warm a candidate before storing it.
`Set` updates resident values, returns `false` for rejected cold candidates,
and never grows beyond `Capacity`. `Stats()` reports hit/miss, admission,
rejection, eviction, counter-table, and aging data. The cache does not add
locks; synchronize it externally when sharing it between goroutines.

The default counter table is the next power of two at least four times the
capacity, with a minimum of 64 counters. Each counter is four bits, so the
sketch itself uses two bytes per counter. Set `CounterCount` to a larger power
of two when a workload has a large scan stream or many hash collisions.

## Measurement

Commands:

```sh
make round77-frequency-admission-baseline-bench
make round77-frequency-admission-bench
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. Both caches have capacity 8
and execute the same 80-operation cycle: 16 hot operations over four keys,
then 64 one-pass scan keys. The baseline is a fixed-capacity LRU with the same
zero-allocation array/list shape.

| Workload | Fixed LRU median | Frequency admission median | Relative result |
| --- | ---: | ---: | ---: |
| Mixed CPU | 36.36 ns/op | 36.15 ns/op | 1.01x faster |
| Hit ratio | 0.1500 | 0.2000 | 1.33x more hits |
| Per-operation heap | 0 B/op, 0 allocs/op | 0 B/op, 0 allocs/op | no allocation change |
| Frequency sketch | not applicable | 2,048 bytes (`CounterCount=4096`) | fixed/bounded |
| Resident-hit fast path | not measured | 5.84 ns/op | zero allocations |

Raw samples (`ns/op`, `hit-ratio`, `B/op`, `allocs/op`):

```text
baseline: 36.10 0.1500 0 0
baseline: 36.36 0.1500 0 0
baseline: 36.27 0.1500 0 0
baseline: 36.76 0.1500 0 0
baseline: 37.28 0.1500 0 0
admission: 35.31 0.2000 0 0
admission: 37.47 0.2000 0 0
admission: 36.15 0.2000 0 0
admission: 36.76 0.2000 0 0
admission: 36.07 0.2000 0 0
resident-hit: 5.69 0 0 0
resident-hit: 5.84 0 0 0
resident-hit: 6.01 0 0 0
resident-hit: 5.60 0 0 0
resident-hit: 5.95 0 0 0
```

The feature is deliberately opt-in. It improves scan resistance at a small
CPU cost on misses and a fixed sketch-memory cost; existing caches and page
indexes do not change behavior until they explicitly construct this type.
