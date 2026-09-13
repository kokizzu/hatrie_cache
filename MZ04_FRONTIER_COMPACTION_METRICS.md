# MZ-04: Frontier Compaction Debt Metrics

Materialize-inspired frontier observability makes retained history visible to
operators without changing compaction policy. `FrontierRetentionRegistry`
already protects historical reads with leases; its snapshot now reports how
far those leases hold compaction behind the current completed frontier.

## API

```go
snapshot, err := retention.Snapshot("orders")
if err != nil {
	return err
}
fmt.Printf("debt=%d blocked=%t blockers=%d\n",
	snapshot.CompactionDebt,
	snapshot.BlockedByLease,
	snapshot.BlockingLeaseCount,
)
```

The additional fields are:

| Field | Meaning |
| --- | --- |
| `CurrentLower` | Current completed lower frontier. |
| `CurrentUpper` | Current available upper frontier. |
| `CompactionDebt` | `CurrentLower - SafeCompactionBefore` when a lease pins history; otherwise zero. This is a logical timestamp distance, not bytes or rows. |
| `BlockedByLease` | Whether an active retention lease prevents compaction from reaching `CurrentLower`. |
| `BlockingLeaseCount` | Number of active leases at the minimum retained timestamp. It is zero when leases exist but do not currently block compaction. |

`SafeCompactionBefore` remains the authoritative boundary. The metrics are
diagnostic and do not trigger, delay, or cancel compaction. The feature is
backward-compatible and does not add a worker, persistent metadata, or
per-advance allocation. Minimum-lease counts are maintained while leases are
acquired and released, so normal snapshot reads stay O(1). Releasing the last
lease at the minimum may scan the remaining leases once to find the next
minimum.

## Example

1. The `orders` frontier reaches lower/upper `100/100`.
2. A reader acquires an as-of lease at `100`.
3. The frontier advances to `120/130`.
4. `Snapshot("orders")` reports `CurrentLower=120`, `CurrentUpper=130`,
   `SafeCompactionBefore=100`, `CompactionDebt=20`,
   `BlockedByLease=true`, and `BlockingLeaseCount=1`.
5. After the reader releases the lease, debt returns to zero and the blocker
   count becomes zero.

## Measurement

The benchmark performs repeated snapshots of one frontier with one active
lease. Five samples, `-benchtime=100000x`, AMD Ryzen 9 5950X:

| Operation | Before | After | Relative change |
| --- | ---: | ---: | ---: |
| Retention snapshot | 57.45 ns/op | 61.53 ns/op | 1.07x CPU, +4.08 ns (+7.1%) |
| Heap allocation | 0 B/op | 0 B/op | unchanged |
| Allocations | 0 allocs/op | 0 allocs/op | unchanged |

The initial implementation scanned active leases on every snapshot and
measured roughly 1.7x CPU; it was replaced with the incremental minimum-count
representation before this feature was retained. Reproduce with:

```text
make benchmark-mz04-before
make benchmark-mz04-after
```

The small fixed read cost buys actionable blocked-frontier diagnostics. No
throughput claim is made for compaction itself.
