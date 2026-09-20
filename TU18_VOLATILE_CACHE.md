# T-U18 Volatile Cache

`VolatileCache[K, V]` is an opt-in, thread-safe, memory-only cache for
ephemeral state. It combines a bounded LRU policy with optional TTL expiry and
optional byte accounting. It deliberately has no persistence, snapshot, or
wire-transfer API.

## Example

```go
cache, err := hatDataStructure.NewVolatileCache[string, []byte](
	 hatDataStructure.VolatileCacheOptions[string, []byte]{
		Capacity: 10000,
		MaxBytes: 64 << 20,
		SizeOf: func(_ string, value []byte) int {
			return len(value)
		},
	},
)
if err != nil {
	return err
}

if err := cache.Set("session:42", payload, 15*time.Minute); err != nil {
	return err
}
value, ok := cache.Get("session:42")
```

`Set` with a zero TTL keeps an item until eviction or deletion. `Get` promotes a
hit to the LRU front; `Peek` reads without changing LRU order. `PurgeExpired`
is the explicit O(n) sweep for idle caches. Expired entries are also removed
lazily when accessed.

## Safety defaults

- `Capacity` must be positive; an unbounded cache cannot be constructed.
- `MaxBytes: 0` means no byte limit. A non-zero byte limit requires `SizeOf`.
- Negative TTLs and negative size results are rejected.
- An item larger than `MaxBytes` is rejected without changing an existing value.
- Capacity and byte-limit evictions remove the least recently used entries.
- Values are stored as supplied; reference values are not deep-copied.

## Operations

| Operation | Behavior |
| --- | --- |
| `Set(key, value, ttl)` | Insert or replace, refresh LRU position, and enforce limits. |
| `Get(key)` | Return a value, count a hit, and promote the entry. |
| `Peek(key)` | Return a value without promoting it; hit/miss counters still update. |
| `Delete(key)` | Remove one entry. |
| `PurgeExpired(now)` | Remove all entries expired at or before `now`. |
| `Clear()` | Remove all entries while retaining cumulative counters. |
| `Stats()` | Return hit, miss, set, delete, eviction, expiration, item, and byte data. |

## Tradeoffs

On the benchmark host (AMD Ryzen 9 5950X), hot operations performed with no
allocations per operation. The raw `map[string]int` baseline was about 7.2
ns/op for `Get` and 10.3 ns/op for `Set`. The volatile cache measured about
16.2 ns/op for `Get`, 12.0 ns/op for `Peek`, 16.9 ns/op for `Set`, and 14.9
ns/op for a TTL-enabled `Get`.

The mutex and LRU bookkeeping therefore make it slower than an unbounded map
on uncontended hot paths. The cache is useful when bounded memory, expiration,
eviction, or concurrent ownership is worth that cost. Use the existing durable
structures when recovery, serialization, or replication is required.
