# T-U13: Durable Cluster Membership Journal

T-U13 adds an opt-in `hatTopology.MembershipJournal` for generation-fenced
cluster join and leave records. It keeps a bounded detached membership
snapshot, retains replayable operation history, and atomically persists each
accepted change when opened with a path.

The journal is deliberately local and transport-neutral. A deployment still
uses its existing consensus or quorum mechanism to authorize a change and
supplies the resulting monotonically increasing fencing token. The journal
prevents stale local state, duplicate retries, and crash-time partial files; it
does not manufacture distributed consensus.

## Example

```go
journal, err := hatTopology.OpenMembershipJournal(
	"/var/lib/hatrie/membership.hmm",
	hatTopology.MembershipJournalOptions{},
)
if err != nil {
	return err
}

record, err := journal.Apply(hatTopology.MembershipChange{
	Operation:          hatTopology.MembershipOperationJoin,
	OperationID:        "join-node-b-20260920-001",
	ExpectedGeneration: journal.Snapshot().Generation,
	FencingToken:       nextConsensusFence,
	Node: hatTopology.TopologyNode{
		ID:      "node-b",
		Address: "10.0.0.12:8080",
		Role:    "replica",
		Region:  "sg",
	},
})
if err != nil {
	return err
}
_ = record
```

`OpenMembershipJournal` restores the previous state when the file exists and
creates an empty state otherwise. `Apply` persists before publishing the new
state in memory. The file uses a versioned `HMM1` frame with a CRC32C checksum,
0600 permissions, a synced temporary file, atomic rename, and directory sync.
`Save` is available for journals created without a configured path.

## Safety Contract

- `ExpectedGeneration` must equal the current generation exactly.
- `FencingToken` must be greater than the current token.
- `OperationID` makes an unchanged retry idempotent while its record remains in bounded history.
- Different content with an existing operation ID is rejected.
- Unknown operations, duplicate node IDs, missing leave targets, and removal of the last node are rejected.
- `Replay` reports a history-gap error after compaction instead of returning an incomplete stream.
- `MaxHistory` defaults to `256` records and is capped at `65,536`.
- `MaxBytes` defaults to `8 MiB` and is capped at `64 MiB`.

History compaction removes the oldest retained records but never changes the
current node set, generation, fencing token, or last sequence. Operation IDs
must therefore remain unique across the deployment's retry window, not only
within the retained history window.

## Tradeoffs

| Concern | Behavior | Operational consequence |
| --- | --- | --- |
| Durability | A successful path-backed `Apply` atomically replaces a CRC-checked snapshot | Membership changes rewrite the bounded state instead of appending an OS-visible partial file |
| Recovery | Open validates the frame, schema, monotone sequence/generation/fence values, and canonical node fields | Corrupt or stale files fail closed and leave no partially restored state |
| Memory | History and nodes are bounded by `MaxHistory`, `MaxBytes`, and node limits | Old operation IDs can be forgotten after history compaction |
| Availability | Generation and fence checks are strict | A stale operator or partitioned node must refresh state before joining/leaving |
| Distributed correctness | Consensus and authorization remain caller-owned | The journal is not a replacement for a consensus service |

## Measurements

Machine: AMD Ryzen 9 5950X, Linux/amd64. Five `-benchmem` samples.

| Path | Median ns/op | B/op | Allocs/op | Comparison |
| --- | ---: | ---: | ---: | --- |
| Existing `ClusterTopology.Clone`, one node | 70.53 | 144 | 1 | baseline detached topology copy |
| `MembershipJournal.Snapshot`, one node | 138.8 | 352 | 2 | 1.97x CPU, +208 B, +1 alloc for generation/history metadata |
| `MembershipJournal.MarshalBinary`, one node | 1,017 | 1,122 | 5 | explicit persistence/CRC cost, not a query-path operation |

The added snapshot cost is paid only by callers that opt into the membership
journal. Existing `ClusterTopology` reads, JSON topology persistence, and
replication defaults are unchanged.

Reproduce the measurements with:

```text
make baseline-tu13-membership
make benchmark-tu13-membership
```
