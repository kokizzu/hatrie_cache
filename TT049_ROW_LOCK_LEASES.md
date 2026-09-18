# TT-049 SQL Row Lock Leases

This feature adopts Tarantool-style serialized row ownership as an importable,
bounded primitive. It is useful for callers implementing `SELECT FOR UPDATE`
workflows around a read followed by a mutation.

## Importable API

```go
manager := hatSql.NewSQLRowLockManager(hatSql.SQLRowLockManagerOptions{})
lease, err := manager.Acquire(ctx, "orders/42")
if err != nil {
    return err
}
defer lease.Release()

// Read and mutate the row while this key is exclusively owned.
```

`Acquire` waits for the same key and returns `context.Canceled` or
`context.DeadlineExceeded` without retaining a canceled waiter. `TryAcquire`
returns `(nil, nil)` for a contended existing key. Different keys can proceed
concurrently across sharded lock maps. `Release` is idempotent and wakes the
next waiter.

## Bounds And Defaults

Zero options use 16 shards, 65,536 live keys, and a 256-byte maximum key. Idle
keys are removed after the final lease/waiter leaves, so short-lived keys do
not accumulate forever. Capacity errors are explicit instead of silently
growing an unbounded map.

The manager is process-local. It provides no distributed fencing, persistence,
replication, or crash recovery. Callers must choose a stable row identity,
hold the lease for the required transaction scope, and use a distributed
coordination mechanism when multiple processes can mutate the same row.

SQL parser syntax and automatic transaction wiring remain caller-owned. The
existing SQL executor and defaults are unchanged.

## Measured Cost

Five 100 ms samples on Linux/amd64 with an AMD Ryzen 9 5950X measured:

| Path | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Sharded row-lock acquire/release | 237.9 | 176 | 3 |
| One uncontended `sync.Mutex` acquire/release | 4.49 | 0 | 0 |

The mutex row is not a replacement comparison: it has no key lookup, bounded
capacity, cancellation, waiter handoff, or lease object. It shows the cost of
keyed ownership so callers can use the primitive only where its semantics are
needed.
