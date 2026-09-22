# T203 Strict Leader Write Fencing

T203 adds an opt-in local write gate that rejects stale writers after a leader
change. A write is admitted only when its node ID, term, and fencing token all
exactly match the currently committed leader credentials.

The gate is deliberately transport-neutral. It does not discover leaders,
persist terms, send replication messages, or change existing write paths. A
caller connects it to the committed result of T202 or another consensus path.

## Defaults and API

The zero-value options are disabled. When enabled, a fence may start with no
leader and accept its first committed transition through `Advance`.

```go
fence, err := hatReplication.NewReplicaSetLeaderWriteFence(
	hatReplication.ReplicaSetLeaderWriteFenceOptions{
		Enabled:             true,
		InitialLeader:       "node-a",
		InitialTerm:         1,
		InitialFencingToken: 1,
	},
)

// Map the committed T202 proposal into the local write fence.
_, err = fence.Advance(hatReplication.ReplicaSetLeaderWriteFenceTransition{
	LeaderID:     proposal.LeaderID,
	Term:         proposal.Term,
	FencingToken: proposal.FencingToken,
})

err = fence.Execute(hatReplication.ReplicaSetLeaderWrite{
	NodeID:       "node-b",
	Term:         2,
	FencingToken: 2,
}, func() error {
	return applyLocalMutation()
})
```

## Safety rules

- `Execute` rejects a missing leader, an unknown/stale node, a lower term, a
  lower token, or any non-exact credential pair with
  `ErrReplicaSetLeaderWriteFenceStale` or `ErrReplicaSetLeaderWriteFenceNoLeader`.
- `Advance` requires a non-empty leader and non-zero term/token. Both term and
  fencing token must be strictly greater than the current values; equal-token
  owner changes are rejected.
- `Advance` takes the exclusive lock while replacing the state. `Execute`
  holds the read lock while its callback runs, so a transition cannot pass an
  already-admitted local callback and a callback admitted after `Advance`
  cannot use the old credentials.
- The callback must perform the actual local mutation and must not call fence
  methods recursively. Remote stores still need to enforce the term/token in
  their own durable write authorization; a process-local gate cannot fence a
  separate service by itself.
- A deployment should commit or durably publish the new consensus credentials
  before exposing the new leader, then call `Advance` before admitting its new
  leader writes. The external consensus/topology operation remains the source
  of truth.

## Measurement

Five `GOMAXPROCS=1` samples were collected with `make benchmark-t203` on Linux
amd64, AMD Ryzen 9 5950X. The plain callback is a lower-bound control, while
the journal-quorum row is an existing three-voter validation control; neither
is an apples-to-apples storage benchmark.

| Workload | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Plain unfenced callback control | 0.261 | 0 | 0 | 1.00x |
| Existing three-voter journal quorum validation | 34.81 | 0 | 0 | 133.4x control |
| Accepted fenced write callback | 13.01 | 0 | 0 | 49.8x control |
| Stale write rejection | 11.65 | 0 | 0 | 44.6x control |
| Leader transition (`Advance`) | 20.27 | 0 | 0 | 77.7x control |

The important result is not the artificial no-op callback ratio: both accepted
and rejected fence paths are allocation-free, and the accepted fence is below
the existing quorum validation cost. The stale rejection path originally
formatted an error and measured about `180 ns/op`, `144 B/op`, and `2 allocs/op`;
returning the stable sentinel removed that avoidable safety-path allocation.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
Plain callback:       0.3047 0.2453 0.2610 0.2632 0.2601; 0; 0
Journal quorum:      34.64 34.78 35.60 34.81 37.40; 0; 0
Accepted fenced:     12.85 13.08 12.55 13.01 13.42; 0; 0
Stale rejection:     11.41 12.39 11.65 11.26 11.80; 0; 0
Leader advance:      19.73 20.54 20.22 20.60 20.27; 0; 0
```

Focused behavior, package, race, vet, and documentation checks are exposed by
the T203 Makefile targets. The red test was run before implementation and
failed on the missing fence API; the final focused test passes.
