# T-U12 Automatic Failover Coordinator

`hatReplication.AutomaticFailoverCoordinator` adds an opt-in policy layer above
the existing election, fencing, and `ReplicaPromotionBarrier` primitives. It
turns one authenticated health observation into a deterministic promotion
proposal, then fences commit or cancellation with a local generation.

Automatic failover is disabled by default. The coordinator has no background
goroutine, network calls, quorum transport, or topology side effects. The
embedding control plane still owns consensus, health authentication, fencing,
and the actual capture/promote/topology commit sequence.

## Policy

When enabled, `Propose` requires:

- the current source is unhealthy;
- the trusted observation has a valid non-zero fencing token and topology
  generation;
- healthy voters meet the configured quorum, or a strict majority when
  `QuorumSize` is zero;
- at least one healthy candidate is within `MaxLag` of the source journal
  sequence; the default lag is zero, requiring an exact match.

The selected candidate has the highest applied sequence. Equal sequences are
resolved by lexical node ID, making repeated observations deterministic. The
proposal increments both the fencing token and topology generation. A stale or
modified proposal cannot be committed, and a committed or cancelled
coordinator cannot be reused for another event.

```go
coordinator, err := hatReplication.NewAutomaticFailoverCoordinator(
	hatReplication.AutomaticFailoverOptions{Enabled: true},
)
if err != nil {
	return err
}

proposal, err := coordinator.Propose(observation)
if err != nil {
	return err
}

// Commit the proposal through the authenticated consensus/topology path.
// Then use the existing ReplicaPromotionBarrier Capture/Promote operations.
_, err = coordinator.Commit(proposal)
```

`AutomaticFailoverObservation` is deliberately caller-supplied. Do not build
it from unauthenticated client input or from one local heartbeat alone. The
caller must establish the voter set, quorum evidence, source health, applied
sequences, and current fencing/topology generations using its control plane.
The coordinator only validates and binds those facts locally.

## Configuration and Safety

`Enabled` defaults to `false`. `QuorumSize == 0` selects `floor(VoterCount/2)+1`.
`MaxLag == 0` is the conservative exact-replay default. `MaxCandidates` defaults
to 128 and is hard-limited to 4096 to bound untrusted or stale observations.

The coordinator is single-use. On a new health event, create a new coordinator
and obtain a new observation and fencing proposal. This prevents a previously
committed decision from being reused after topology changes.

This feature does not make a partition safe by itself. A caller must serialize
proposal commit through a quorum or consensus authority, fence the old source,
and publish the new topology atomically. If those controls are unavailable,
leave `Enabled` false and use the existing manual promotion barrier.

## Measurement

Five `-benchmem` samples on an AMD Ryzen 9 5950X, Linux/amd64:

| Operation | Median | Allocations | Reference |
| --- | ---: | ---: | --- |
| Simple eligibility baseline | 0.482 ns/op | 0 B/op, 0 allocs/op | Lower-bound control |
| Full policy evaluation | 84.53 ns/op | 0 B/op, 0 allocs/op | 175x control cost |
| Coordinator `Snapshot` | 10.79 ns/op | 0 B/op, 0 allocs/op | Allocation-free status path |

The policy runs during a failover decision, not on the request or replication
hot path. Candidate validation costs about 85 ns with two candidates and does
not allocate. The status path is allocation-free after the inline proposal
change. Existing `ReplicaPromotionBarrier` execution remains caller-owned.

Run the reproducible benchmark with:

```text
make benchmark-tu12-dev
```

Raw samples are included in [BENCHMARK.md](BENCHMARK.md#t-u12-automatic-failover-coordinator).
