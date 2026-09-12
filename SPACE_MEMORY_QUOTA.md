# Per-Space Memory Quotas

`hatStorage` now provides an opt-in Tarantool-style named-space memory budget.
`SpaceMemoryQuota` is a small atomic admission counter and
`SpaceMemoryQuotaRegistry` maps names to reusable quota handles. A caller
reserves the bytes it is about to allocate and releases them when that memory is
freed.

The feature is off by default. No registry or quota handle is created by the
existing cache/storage paths, and it does not inspect or intercept Go heap
allocations automatically.

## Example

```go
registry := hatStorage.NewSpaceMemoryQuotaRegistry()
orders, err := registry.Register("orders", 64<<20)
if err != nil {
	return err
}

payloadBytes := uint64(len(payload))
if err := orders.Reserve(payloadBytes); err != nil {
	if errors.Is(err, hatStorage.ErrSpaceMemoryQuotaExceeded) {
		return ErrBackpressure
	}
	return err
}
if err := store(payload); err != nil {
	_ = orders.Release(payloadBytes)
	return err
}
// Release when the stored payload is evicted or deleted.
_ = orders.Release(payloadBytes)
```

Use `Lookup` once and retain the returned handle on hot paths. Registry
`Reserve`/`Release` convenience methods are useful for infrequent operations,
but take a read lock for each name lookup.

## Semantics

| Operation | Behavior |
| --- | --- |
| `NewSpaceMemoryQuota(name, limit)` | Creates an immutable quota handle |
| `Reserve(bytes)` | Atomically admits bytes or rejects without waiting |
| `Release(bytes)` | Atomically returns bytes and rejects underflow |
| `Limit()` | Returns the configured maximum; `0` means unlimited |
| `Used()` / `Available()` | Reports current accounting, with overflow-safe arithmetic |
| `Snapshot()` | Returns one owned point-in-time report |
| `Register(name, limit)` | Adds a named handle and rejects duplicates |

Limits are immutable so the reserve CAS loop cannot race a live limit change.
For configuration reloads, construct a new registry and hand new operations to
the new handles after the old space has drained. Existing handles keep their
original contract.

For a bounded quota, `Reserve` must happen before the allocation. For a
multi-space operation, reserve every space first and roll back successful
reservations if a later one fails. Release exactly the amount that was
reserved. `ErrSpaceMemoryQuotaUnderflow` and
`ErrSpaceMemoryQuotaOverflow` make accounting bugs visible instead of silently
weakening the limit.

Zero limit means unlimited admission, but usage is still tracked until the
uint64 accounting range is exhausted. This is useful for instrumentation but is
not a substitute for a bounded limit.

## Reporting

```go
for _, space := range registry.Snapshot() {
	fmt.Printf("%s: %d/%d bytes\n", space.Name, space.UsedBytes, space.LimitBytes)
}
// orders: 1048576/67108864 bytes
```

The registry snapshot is sorted by name and contains independent report values.
It is suitable for an admin endpoint, metrics exporter, or periodic log line.

## Measurements

Measured on Linux/amd64, AMD Ryzen 9 5950X, with
`make benchmark-t-u43` (`go test -benchmem -count=5`).

| Workload | Result |
| --- | ---: |
| Direct handle `Reserve(64)` + `Release(64)` | 4.33 ns/op, 0 B/op, 0 allocs/op |
| Registry lookup + reserve/release pair | 29.08 ns/op, 0 B/op, 0 allocs/op |
| Disabled/default path | 0 calls and 0 added work |

The direct handle is the intended hot path. The registry convenience path is
roughly 6.7x slower because it performs two map lookups and read-lock pairs, but
it still allocates nothing. Tests also run 20 concurrent 100-byte reservations
against a 1,000-byte quota and verify exactly 10 admissions with no
oversubscription.

The quota counts caller-declared logical bytes, not every allocator header,
map bucket, goroutine stack, or unrelated runtime allocation. Integrations that
need a hard process RSS ceiling should combine this admission layer with the
existing process-level memory controls and eviction policy.

## Verification

```text
make test-t-u43
make benchmark-t-u43
```

Tests cover capacity rejection, release and overflow/underflow errors, unlimited
mode, registry lookup/duplicate handling, deterministic snapshots, and
concurrent oversubscription prevention.
