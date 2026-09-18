# TR003 Replica-Promotion Catch-Up Barrier

`hatReplication.ReplicaPromotionBarrier` adopts Tarantool-style promotion
catch-up fencing. A control-plane caller reports the source journal sequence
and each replica's applied sequence. Promotion is allowed only after the
candidate reaches the exact source fence captured for that promotion.

```go
barrier, err := hatReplication.NewReplicaPromotionBarrier(
	hatReplication.ReplicaPromotionBarrierOptions{MaxReplicas: 64},
)
if err != nil {
	return err
}
if err := barrier.ObserveSource(sourceSequence); err != nil {
	return err
}
if err := barrier.ObserveReplica("standby-a", appliedSequence); err != nil {
	return err
}

// Stop or fence the old primary before capturing the exact promotion fence.
token, err := barrier.Capture("standby-a")
if err != nil {
	return err // the standby is behind the source fence
}
result, err := barrier.Promote(token)
if err != nil {
	return err
}
_ = result.Generation // carry this generation into topology activation
```

`Capture` rejects an unknown or lagging replica. If the source sequence moves
between `Capture` and `Promote`, the token is rejected and the caller must
observe progress and capture again. A successful `Promote` advances the
generation, making the token single-use. Regressing source or replica
sequences are rejected.

The barrier is deliberately transport-neutral and local. It does not run an
election, stop the old primary, replicate journal data, or persist topology.
The caller must fence the old writer and commit the returned generation through
its shared topology/control-plane protocol. This keeps the default single-node
and existing replication paths unchanged.

## Memory And Concurrency

Progress snapshots are immutable and published through an atomic pointer.
Control-plane updates are serialized; capture, promotion, and snapshots can be
used concurrently. Capture and successful promotion allocate zero bytes.
Replica observations copy the bounded map, so they should be reported at
replication-status cadence rather than for every row.

The default limit is 64 replica IDs and the hard limit is 1,048,576. Replica
IDs are normalized by trimming surrounding whitespace and must be non-empty.
Do not copy a barrier after first use.

## Measurement

The measurements use Linux/amd64 on an AMD Ryzen 9 5950X with five samples.
The manual control performs the same source, node, applied-sequence, and
generation comparisons using a map, but has no immutable publication or
single-use generation transition.

| Operation | Median ns/op | B/op | allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Manual map capture | 7.660 | 0 | 0 | 1.00x |
| Barrier capture | 14.13 | 0 | 0 | 1.84x higher CPU, same memory |
| Manual map promotion | 7.832 | 0 | 0 | 1.00x |
| Barrier promotion | 36.65 | 0 | 0 | 4.68x higher CPU, same memory |
| Barrier replica observation | 243.7 | 272 | 3 | Bounded control-plane update |
| Barrier snapshot | 120.8 | 48 | 2 | Detached inspection |

The raw check is faster, but it cannot invalidate previously captured tokens
or atomically advance a promotion generation. The barrier's absolute promotion
cost is tens of nanoseconds and is paid once per failover, not per replicated
record. The extra observation allocations are bounded and intentionally kept
off the record-apply path.

Run the benchmark with:

```text
make benchmark-tr003-replica-promotion-barrier
```

Raw final samples (`ns/op`, `B/op`, `allocs/op`):

```text
manual-capture: 7.421 0 0
manual-capture: 8.177 0 0
manual-capture: 7.768 0 0
manual-capture: 7.660 0 0
manual-capture: 7.515 0 0
manual-promote: 7.647 0 0
manual-promote: 7.662 0 0
manual-promote: 7.832 0 0
manual-promote: 8.348 0 0
manual-promote: 7.930 0 0
barrier-capture: 13.68 0 0
barrier-capture: 14.54 0 0
barrier-capture: 14.65 0 0
barrier-capture: 14.13 0 0
barrier-capture: 13.82 0 0
barrier-promote: 36.72 0 0
barrier-promote: 36.88 0 0
barrier-promote: 35.19 0 0
barrier-promote: 36.65 0 0
barrier-promote: 35.90 0 0
barrier-observe-replica: 244.3 272 3
barrier-observe-replica: 236.8 272 3
barrier-observe-replica: 243.7 272 3
barrier-observe-replica: 240.3 272 3
barrier-observe-replica: 257.5 272 3
barrier-snapshot: 120.8 48 2
barrier-snapshot: 121.6 48 2
barrier-snapshot: 116.0 48 2
barrier-snapshot: 119.3 48 2
barrier-snapshot: 122.8 48 2
```
