# T-U46 Index Cardinality And Hot-Key Statistics

`hat/hatIndexStats` is an opt-in diagnostic tracker for ordered, hash, or
posting-list indexes. It records exact operation/hit/miss counters, a bounded
64-register approximate cardinality estimate, maximum posting length, sampled
posting totals, and a deterministic bounded heavy-hitter list.

```go
tracker, _ := hatIndexStats.New(hatIndexStats.DefaultConfig())
tracker.Observe(key, postingLength, hit)
snapshot := tracker.Snapshot()
```

The default configuration samples every 16 observations, retains eight hot
keys, and copies keys up to 128 bytes. Long keys remain hash-only. `Snapshot`
returns isolated key buffers and `Reset` reuses the configured backing storage.
All observation modes are allocation-free after construction; snapshots are
intentionally more expensive and should be used for diagnostics, not per-row
execution.

Five-run medians on the local Ryzen 9 5950X:

| Operation | CPU | Memory | Allocations |
|---|---:|---:|---:|
| Direct atomic counter baseline | 1.81 ns/op | 0 B/op | 0 |
| Sampled observation, every 16th key | 9.34 ns/op | 0 B/op | 0 |
| Observation sampled every key | 14.07 ns/op | 0 B/op | 0 |
| Eight-entry snapshot | 1.03 us/op | 648 B/op | 12 |

The tracker is deliberately opt in: callers pay the lock, hash, and cardinality
update cost only when they attach diagnostics. Cardinality and hot-key counts
are approximate; exact operation and hit/miss counters are provided for
operator interpretation.
