# T207 Replica Eviction, Rejoin, and Stale-State Recovery

T207 adds an opt-in, transport-neutral lifecycle around
`hatReplication.ReplicaJoinAdmission`:

- `Evict` removes an active member only when the caller presents the member's
  exact generation.
- Every eviction creates a bounded identity tombstone with a monotone
  `EvictionEpoch`.
- A tombstoned identity cannot use ordinary `Prepare`; it must use
  `PrepareRejoin` with the exact epoch.
- `CommitRejoin` requires the exact bootstrap plan to be active and caught up,
  and requires the decision's topology generation to still be next.
- `AbortRejoin` cancels only the pending decision; it keeps the tombstone.
- `RecoverySnapshot` returns a detached, node-sorted view for monitoring and
  persistence adapters.

The registry does not perform network I/O, copy snapshots, apply WAL records,
authenticate peers, or publish routes. The embedding control plane owns those
side effects and must pass the resulting `SnapshotWALBootstrapState` to the
commit method.

## Example

```go
eviction, err := admission.Evict("node-a", member.Generation, "replace disk")
if err != nil {
	return err
}

decision, err := admission.PrepareRejoin(hatReplication.ReplicaRejoinRequest{
	ReplicaJoinRequest: request,
	EvictionEpoch:     eviction.EvictionEpoch,
})
if err != nil {
	return err
}

// Install the exact decision.BootstrapPlan and advance it through the existing
// SnapshotWALBootstrapCoordinator before publishing the member again.
member, err = admission.CommitRejoin(decision, activeBootstrapState)
```

An address may change during recovery, but the stable node identity and
eviction epoch cannot. Active, pending, and retained-eviction addresses are
checked for collisions. A normal join attempt for a tombstoned identity is
rejected so an old process cannot bypass the rejoin protocol.

## Bounded Defaults

`ReplicaJoinAdmissionOptions.MaxEvictions` defaults to `256` and is capped at
`65,536`. The registry rejects a new eviction when its tombstone capacity is
full rather than pruning a fence that could still be needed to reject stale
state. A successful rejoin removes its own tombstone and frees one slot.

The epoch is a control-plane stale-state fence, not a cryptographic credential.
Peer authentication and authorization must protect the transport that carries
the eviction and rejoin messages.

## Measurements

Five benchmark samples were collected on Linux/amd64 with an AMD Ryzen 9 5950X.
The ordinary join retry is the existing T206 `Prepare` idempotency path; the
rejoin retry is the corresponding T207 path. The snapshot comparison uses 128
entries in both cases.

| Workload | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing ordinary join retry | 662.4 | 296 | 4 | 1.00x |
| T207 rejoin retry | 800.0 | 328 | 4 | 1.21x |
| Existing 128-member snapshot | 14,138 | 13,184 | 2 | 1.00x |
| T207 128-eviction recovery snapshot | 13,661 | 13,184 | 2 | 0.97x |

The rejoin overhead is accepted because it is a bounded control-plane safety
check, not a normal write or WAL-apply path. Snapshot CPU is within benchmark
noise and uses identical memory and allocation counts.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
Existing join retry:       663.8 627.4 631.2 662.4 665.6; 296; 4
T207 rejoin retry:          810.8 809.6 794.1 770.8 800.0; 328; 4
Existing snapshot 128:    14138 14152 13857 14141 14085; 13184; 2
T207 recovery snapshot:   13958 13361 14301 13661 12850; 13184; 2
```

Run the focused verification with:

```text
make test-t207
make race-t207
make vet-t207
make benchmark-t207-baseline
make benchmark-t207
```
