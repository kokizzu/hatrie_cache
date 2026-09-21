# TT-005 Raft-Style Configuration State

`hatReplication.RaftConfigurationState` provides the membership-state portion
of Raft-style configuration changes without taking ownership of elections,
transport, durable log replication, or serving-topology publication.

```go
state, err := hatReplication.NewRaftConfigurationState(
	hatReplication.RaftConfigurationStateOptions{
		InitialVoters:   []string{"node-a", "node-b", "node-c"},
		InitialLearners: []string{"node-d"},
	},
)
if err != nil {
	return err
}

entry, err := state.Propose(hatReplication.RaftConfigurationChange{
	AddVoters:      []string{"node-d"},
	RemoveLearners: []string{"node-d"},
}, term)
if err != nil {
	return err
}

snapshot, err := state.Commit(entry, acknowledgements)
```

Every proposal records the previous and next voter sets, a monotonic log index,
term, and generation. Commit requires accepted acknowledgements from a
majority of both the previous and next voter sets, so a membership change
cannot be committed by only one side of a joint configuration. Learners are
tracked for state transfer but never count toward quorum. Promotion and
demotion can be expressed in one transition by removing the old role and
adding the new role together.

The state is bounded by `MaxMembers` (64 by default), canonicalizes and sorts
member IDs, rejects duplicate or overlapping roles, and detaches all snapshots
and entries. A single pending transition is allowed; stale term, generation,
index, node, duplicate-acknowledgement, and quorum inputs fail closed.

This is intentionally opt-in and transport-neutral. The caller must persist or
replicate entries, run elections, fence old leaders, and publish an activated
configuration. Existing quorum and replication behavior is unchanged.
