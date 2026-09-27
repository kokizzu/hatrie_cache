# M033f Durable Global Timestamp Coordinator

`hatReplication.DurableGlobalTimestampOracle` is an opt-in durability wrapper
around the in-memory global timestamp oracle. It persists the candidate
snapshot before publishing the candidate in memory, so a successful `Reserve`
or `AdvanceTerm` is recoverable after process restart.

## Use

```go
coordinator, err := hatReplication.NewDurableGlobalTimestampOracle(
	"/var/lib/hatrie-cache/global-timestamps.bin",
	1,
	0,
)
if err != nil {
	return err
}

grant, err := coordinator.Reserve(hatReplication.GlobalTimestampRequest{
	Term: 1, NodeID: "node-a", NodeEpoch: 1, Sequence: 1, Count: 1024,
})
if err != nil {
	return err
}
_ = grant
```

On restart, the term and initial timestamp arguments are used only when the
file does not exist. Existing snapshots are restored after validation. A
failed write does not publish the candidate state in memory.

## Performance and tradeoff

Measured with:

```text
make benchmark-m033-global-timestamps
```

on the local AMD Ryzen 9 5950X test host, five 250 ms samples:

| Path | Result | Allocations | Meaning |
| --- | ---: | ---: | --- |
| in-memory reserve range | 52.26–61.80 ns/op | 0 B, 0 allocs/op | existing default path |
| durable reserve, count 64 | 3.09–3.57 ms/op | 3,052 B, 29 allocs/op | snapshot, atomic rename, and sync per range |
| in-memory leased next | 2.38–2.66 ns/op | 0 B, 0 allocs/op | existing hot path after a range is granted |

The durable path is therefore roughly 50,000–68,000 times slower per persisted
range than the in-memory range reservation. This is expected filesystem
durability cost, not a replacement for the default hot path. Use a sufficiently
large `Count`, then consume the returned range locally, when durability is
needed and the workload can amortize one durable checkpoint over many writes.

## Ownership and security

This type provides durable publication for one coordinator process. It does
not implement consensus, leader election, fencing across processes, or
replication transport. The caller must ensure that only the elected owner can
write the snapshot path and must coordinate term changes with the cluster
membership protocol.

The snapshot format includes integrity validation, but its checksum is not an
authentication mechanism. Keep the file in a protected directory with
appropriate filesystem ownership and permissions, and do not expose it as a
client-controlled path.
