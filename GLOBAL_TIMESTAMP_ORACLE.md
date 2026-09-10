# Global Timestamp Oracle

`hatReplication.GlobalTimestampOracle` is a transport-neutral state machine for
globally ordered write timestamps. One consensus-selected coordinator instance
serializes reservations from every node and returns non-overlapping contiguous
ranges. `GlobalTimestampLease` consumes a range locally without allocations.

```go
oracle, err := hatReplication.NewGlobalTimestampOracle(7, 0)
if err != nil {
	return err
}

grant, err := oracle.Reserve(hatReplication.GlobalTimestampRequest{
	Term:      7,
	NodeID:    "node-a",
	NodeEpoch: 12, // from PersistentNodeEpoch
	Sequence:  1,
	Count:     1024,
})
if err != nil {
	return err
}
lease, err := grant.Lease()
if err != nil {
	return err
}
timestamp, ok := lease.Next()
```

## Safety Contract

- `Term` fences an old coordinator after a leader or consensus-term change.
- `NodeEpoch` fences a restarted writer. A newer epoch must begin at sequence
  one; older epochs are rejected.
- `Sequence` makes a retry return the exact prior grant. Reusing a sequence with
  a changed count or observed timestamp is rejected, and gaps are rejected.
- `Observed` advances the coordinator floor before allocation, so a timestamp
  received from another source cannot be followed by an older local grant.
- Ranges never overlap. Unused values after a process failure remain gaps and
  are never reused.
- `Snapshot` and `NewGlobalTimestampOracleFromSnapshot` preserve the bounded
  one-record-per-node retry state and reject malformed or overlapping ranges.

The coordinator must be placed behind the caller's consensus or trusted
single-writer control plane. Publish the oracle snapshot atomically with the
consensus log or durable state, and require every globally ordered writer to
obtain a grant before publishing its write. This package does not implement
leader election, network transport, authentication, or durable commit coupling.
The existing process-local `TimestampOracle` remains the lower-cost choice when
cross-node ordering is not required.

## Benchmark

Command: `make benchmark-m033-global-timestamps`.

Five `-benchmem` samples were run on Linux/amd64 with an AMD Ryzen 9 5950X.
The local baseline was measured before the global coordinator was added; the
final local row was rerun beside the new paths. The range rows are warmed so
the map and lease setup are outside the timed operation.

| Path | Raw ns/op (5 runs) | Median ns/op | Median B/op | Median allocs/op | Comparison |
| --- | --- | ---: | ---: | ---: | --- |
| Existing local `TimestampOracle.Next`, before | 2.593; 2.467; 2.510; 2.121; 2.072 | 2.467 | 0 | 0 | baseline |
| Existing local `TimestampOracle.Next`, final | 2.281; 2.272; 2.267; 2.287; 2.275 | 2.275 | 0 | 0 | baseline |
| Global coordinator, one timestamp grant | 55.54; 55.24; 55.08; 55.19; 54.76 | 55.19 | 0 | 0 | 24.26x local cost; global ordering |
| Global coordinator, 1,024-timestamp grant | 55.08; 54.74; 54.91; 54.65; 54.79 | 54.79 | 0 | 0 | 0.0535 ns/timestamp coordination amortization |
| Local consumption from 1,024-timestamp grant | 2.290; 2.275; 2.264; 2.276; 2.282 | 2.276 | 0 | 0 | 1.00x local cost |

The `leased_next` benchmark includes a fresh global reservation and lease reset
once per 1,024 timestamps, so its `2.276 ns/op` result includes the amortized
coordinator work. Range leasing therefore retains global-ordering semantics at
effectively the same steady-state CPU, memory, and allocation cost as the local
oracle. Single-value reservations remain available for strict per-write
coordination but are intentionally slower.

## Verification

```text
make test-m033-global-timestamps
make test-race-m033-global-timestamps
make benchmark-m033-global-timestamps
```
