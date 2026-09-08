# Timestamp Oracle

`hatReplication.NewTimestampOracle` provides a process-local, monotone
Lamport-style timestamp source for conflict versions and other ordered local
events.

```go
oracle, err := hatReplication.NewTimestampOracle(0)
if err != nil {
	return err
}

timestamp, err := oracle.Next()
if err != nil {
	return err
}
version := hatReplication.ConflictVersion{
	Timestamp: timestamp,
	NodeID:    "node-a",
	Sequence:  1,
}
```

After receiving a remote timestamp, call `Observe` before allocating the next
local one:

```go
if err := oracle.Observe(remoteVersion.Timestamp); err != nil {
	return err
}
next, err := oracle.Next()
```

`Next` is linearizable and returns unique values for concurrent callers.
`Observe` only advances the clock, so older or equal observations are ignored.
The clock rejects negative timestamps and reports overflow rather than
wrapping. `Current` reads the greatest allocated or observed value.

This is an in-memory building block. It does not persist across process
restarts, coordinate independent nodes, or establish a global total order when
nodes have not exchanged timestamps. Combine it with
[`PersistentNodeEpoch`](PERSISTENT_NODE_EPOCHS.md) for local restart fencing,
and use a consensus-backed coordinator when the application requires global
ordering across machines. `ConflictVersion.NodeID` remains the deterministic
tie-breaker for concurrent writers.

## Benchmark

Five `-benchmem` samples on an AMD Ryzen 9 5950X, `linux/amd64`:

| Operation | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| `TimestampOracle.Next` | 2.274 | 0 | 0 |

Raw output:

```text
2.482, 2.508, 2.172, 2.274, 2.101 ns/op; 0 B/op; 0 allocs/op
```

## Verification

```sh
make test-timestamp-oracle-local-clean
make test-timestamp-oracle-race-local-clean
make vet-timestamp-oracle-local-clean
make benchmark-timestamp-oracle-local-clean
```
