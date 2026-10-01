# T-U13 Durable Cluster Membership

`hatTopology.MembershipLog` is an opt-in membership state machine for join and
leave changes. It builds on the existing topology fingerprint, fencing-token,
and quorum-decision primitives.

## Contract

1. `ProposeJoin` and `ProposeLeave` validate the requested operation without
   mutating the current topology.
2. The caller collects votes with `EvaluateTopologyConsensus`.
3. `Commit` validates the quorum decision, expected fingerprint, candidate
   fingerprint, monotonic fencing generation, and the operation again before
   applying it.
4. With `MembershipLogOptions.Path`, a successful commit is written through
   the existing fsync, atomic-rename JSON writer. A failed write rolls back the
   in-memory topology, generation, and audit record.
5. `LoadMembershipLog` validates the bounded snapshot before it becomes live.

Joins add an unassigned node, defaulting its role to `replica`. Leaves reject
nodes that are still primary or replica owners of an explicit shard. Membership
does not migrate buckets or change shard ownership; those remain separate
partition-management operations, consistent with the project's preference for
explicit regional partitioning over automatic sharding.

## Defaults And Limits

- The feature is disabled unless a caller constructs `MembershipLog`.
- Automatic file persistence is disabled when `Path` is empty.
- The audit log retains up to 1,024 records by default.
- A snapshot is limited to 4 MiB and the retained record count to 1,048,576.
- JSON decoding rejects unknown fields, trailing values, invalid topology,
  invalid generations, duplicate quorum evidence, and a final record that does
  not describe the current topology.

## Benchmark

The benchmark compares a direct normalized topology append with one in-memory
membership proposal plus quorum validation and commit. It is a control-plane
comparison, not a data-path claim.

| Operation | Direct baseline | Membership log | Tradeoff |
| --- | ---: | ---: | ---: |
| Join proposal and commit | 609.1 ns/op; 1,064 B/op; 7 allocs/op | 8,407 ns/op; 6,576 B/op; 136 allocs/op | 13.80x latency; 6.18x bytes; 19.43x allocs |
| 32-record snapshot marshal | N/A | 32,699 ns/op; 30,688 B/op; 69 allocs/op | bounded persistence serialization |

Raw five-sample output:

```text
BaselineTopologyJoin: 609.1; 594.5; 613.3; 596.6; 609.5 ns/op; 1,064 B/op; 7 allocs/op
MembershipJoinCommit: 8608; 8353; 8350; 8470; 8407 ns/op; 6,576 B/op; 136 allocs/op
MembershipSnapshotMarshal: 32722; 32699; 32301; 32646; 33394 ns/op; 30,679; 30,698; 30,688; 30,688; 30,682 B/op; 69 allocs/op
```

The overhead is intentional and acceptable for infrequent membership changes:
the feature is opt-in, adds no work to ordinary reads or writes, and prevents
stale or non-quorum membership changes from being activated. It would be a bad
replacement for a per-request or per-key path.

## Verification

- `make test-tu13-membership`
- `make verify-tu13-membership`
- `make benchmark-tu13-membership`

The verification target runs the full `hatTopology` package, a focused race
run, and `go vet`.
