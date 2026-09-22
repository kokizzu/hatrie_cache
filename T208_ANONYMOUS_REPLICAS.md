# T208 Anonymous Replicas

T208 adds an explicit membership role for replicas that may serve reads,
bootstrap sources, and health reporting without participating in quorum
decisions.

## Roles

`ReplicaRoleVoter` is the zero value, so existing join requests remain voters.
Set `ReplicaJoinRequest.Role` to `ReplicaRoleAnonymous` to opt a new member
out of quorum participation:

```go
request.Role = hatReplication.ReplicaRoleAnonymous
decision, err := admission.Prepare(request)
if err != nil {
	return err
}
member, err := admission.Commit(decision, activeBootstrapState)
```

`ReplicaJoinMember.Role` records the committed role. T208 also preserves that
role in `ReplicaEviction`; a rejoin that tries to change a member from voter to
anonymous, or vice versa, is rejected with
`ErrReplicaJoinAdmissionRoleMismatch`. Role changes therefore require an
explicit future membership transition rather than being smuggled through a
stale recovery request.

## Quorum Boundary

`ReplicaJoinAdmission.RoleRoster` returns a detached, node-sorted
`ReplicaRoleRoster`:

```go
roster := admission.RoleRoster()
result, err := hatReplication.ExecuteVoterWriteQuorum(
	ctx,
	roster,
	[]string{"voter-a", "voter-b", "reader-a"},
	2,
	writeToReplica,
)
```

The call rejects `reader-a` with `ErrReplicaQuorumAnonymousTarget` before any
callback runs. Unknown targets are rejected with
`ErrReplicaQuorumUnknownTarget`. Callers that want the anonymous replica for a
read or bootstrap source continue to use the existing read-selection and join
candidate APIs; only quorum admission is restricted.

The role roster is a snapshot, not a background service. The embedding control
plane must publish a new roster after a membership commit and use its
generation when coordinating external quorum or election state. The existing
`ReplicaSetLeaderElection` remains fixed-voter and can be constructed from
`roster.Voters`; anonymous IDs are never included.

## Safety And Bounds

- Existing callers keep voter behavior because the role zero value is voter.
- Active membership is bounded by `ReplicaJoinAdmissionOptions.MaxMembers`.
- Pending joins and evicted tombstones are excluded from the active roster.
- Role lists are sorted, detached, and validated for empty IDs, duplicates, and
  voter/anonymous overlap.
- `ExecuteVoterWriteQuorum` rejects anonymous or unknown nodes before launching
  the existing concurrent callbacks.
- Roles are not authentication. Transport identity, authorization, and
  fencing remain the embedding service's responsibility.

## Measurements

Five samples were collected on Linux/amd64 with an AMD Ryzen 9 5950X.

| Workload | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing all-target quorum executor | 1,493 | 544 | 10 | 1.00x |
| T208 voter-only quorum executor | 1,675 | 544 | 10 | 1.12x |
| Existing 128-member snapshot | 13,646 | 14,592 | 2 | 1.00x |
| T208 128-member role roster | 11,371 | 6,912 | 3 | 0.83x |

The quorum wrapper adds 12.2% CPU in this small control-path workload without
adding memory or allocations. That is an intentional safety cost: it rejects
misclassified quorum targets before launching callbacks. The role roster uses
one extra allocation but retains only node IDs, so it uses 52.6% less memory
than the full membership snapshot; the payloads are not identical and the
comparison is documented as an export-cost measurement, not an end-to-end
replication speed claim.

Raw samples (`ns/op`, `B/op`, `allocs/op`):

```text
Existing quorum:       1527 1534 1493 1427 1461; 544; 10
T208 voter quorum:      1691 1664 1727 1663 1675; 544; 10
Existing snapshot 128: 13615 13950 13820 13646 13631; 14592; 2
T208 role roster 128:   11353 11299 11978 11425 11371; 6912; 3
```

Run the focused verification with:

```text
make test-t208
make test-t207
make test-t206
make race-t208
make vet-t208
make benchmark-t208-baseline
make benchmark-t208
```
