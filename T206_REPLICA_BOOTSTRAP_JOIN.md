# T206 Deterministic Replica Bootstrap and Join

`hatReplication.ReplicaJoinAdmission` adds the identity and topology admission
layer missing from the lower-level `SnapshotWALBootstrapCoordinator`. It keeps
join decisions deterministic and bounded without performing network I/O,
copying snapshot files, replaying WAL records, or publishing routes itself.

## Workflow

1. Create one admission registry for the control plane.
2. Submit a `ReplicaJoinRequest` containing the joiner's stable node ID,
   endpoint address, snapshot identity, storage generation, target journal
   sequence, fencing token, and candidate sources.
3. Call `Prepare`. It validates identity/address uniqueness, normalizes and
   sorts candidates, and returns an immutable `ReplicaJoinDecision`.
4. Use `decision.BootstrapPlan` with the existing snapshot/WAL coordinator.
5. Install the exact snapshot, replay through the target journal sequence,
   mark ready, and activate the bootstrap.
6. Pass the active bootstrap state to `Commit`. Only then is the member added
   to the admission topology.

```go
admission, err := hatReplication.NewReplicaJoinAdmission(
	hatReplication.ReplicaJoinAdmissionOptions{},
)
decision, err := admission.Prepare(hatReplication.ReplicaJoinRequest{
	JoinerID:              "node-eu-2",
	Address:               "https://node-eu-2",
	SnapshotID:            "snapshot-42",
	StorageGeneration:     7,
	TargetJournalSequence: 1200,
	FencingToken:          11,
	Candidates: []hatReplication.ReplicaJoinCandidate{
		{
			NodeID:                  "node-eu-1",
			Address:                 "https://node-eu-1",
			Healthy:                 true,
			StorageGeneration:       7,
			SnapshotJournalSequence: 1100,
			AppliedJournalSequence:  1195,
			AvailableThrough:        1200,
		},
	},
})

bootstrap, _ := hatReplication.NewSnapshotWALBootstrapCoordinator(
	hatReplication.SnapshotWALBootstrapOptions{},
)
_, _ = bootstrap.Begin(decision.BootstrapPlan)
_, _ = bootstrap.InstallSnapshot("snapshot-42", 7, 1100, 11)
state, _ := bootstrap.AdvanceWAL(1200, 11)
state, _ = bootstrap.MarkReady(state.Generation, 11)
state, _ = bootstrap.Activate(state.Generation, 11)
member, err := admission.Commit(decision, state)
```

Production callers must handle every returned error and perform the actual
snapshot/WAL side effects between the coordinator transitions. The admission
layer intentionally does not hide those boundaries.

## Deterministic Rules

Candidates are eligible only when they are healthy, use the requested storage
generation, have a snapshot sequence no later than the target, and advertise
WAL availability through the target. The source with the greatest applied
sequence wins; equal progress is resolved by the lexicographically smallest
node ID. Candidate order in the request therefore cannot change the decision.

The registry rejects duplicate node IDs and endpoint addresses across active
or pending joins. Retrying the exact pending request returns the same decision;
changing any field returns a conflict. `Commit` requires the exact bootstrap
plan, an active and caught-up bootstrap state, and the next topology
generation. `Abort` removes a pending decision without publishing a member.

## Bounds and Defaults

The zero-value options allow up to 256 active or pending members and 64 source
candidates per request. Hard bounds are 65,536 members and 4,096 candidates.
Node IDs, addresses, and snapshot IDs are length-bounded UTF-8 values. The
registry is not automatically connected to HTTP, gRPC, or topology services;
existing deployments retain their current behavior until they construct and
wire this coordinator.

## Measured Cost

Linux/amd64, AMD Ryzen 9 5950X, five benchmark samples:

| Workload | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Existing raw sequence admission control | 1.826 | 0 | 0 |
| Existing `SnapshotWALBootstrapCoordinator.AdvanceWAL` | 26.27 | 0 | 0 |
| T206 `Prepare` + `Abort` | 959.7 | 680 | 5 |
| T206 snapshot of 128 members | 15,108 | 13,185 | 2 |

These costs are intentionally paid only at join and monitoring control-plane
boundaries, never on normal writes or WAL application. Run the measurements
with `make benchmark-t206-baseline` and `make benchmark-t206`.
