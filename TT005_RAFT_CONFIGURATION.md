# TT-005 Raft Configuration State

Status: partially adopted.

`hatTopology.RaftConfigurationState` is a transport-neutral membership state
machine for Raft-style configuration changes. It supports stable voters,
learners, voter promotion/removal, ordered term/index application, and joint
consensus. During a voter change, acknowledgements must satisfy a majority of
both the old and new voter sets before the configuration can be finalized.

```go
state, err := hatTopology.NewRaftConfigurationState([]string{"a", "b", "c"})
joint, err := state.ProposeMembershipChange(7,
    hatTopology.RaftConfigurationChange{
        Kind:   hatTopology.RaftConfigurationAddVoter,
        NodeID: "d",
    })
err = state.Apply(joint)
decision, err := state.Snapshot().EvaluateQuorum([]string{"a", "b", "d"})
final, err := state.ProposeMembershipFinalize(7)
err = state.Apply(final)
```

The state machine rejects duplicate or unknown members, stale indexes, term
regressions, voter changes that skip joint consensus, duplicate proposals,
unknown quorum acknowledgements, and learner/voter overlap. Snapshots and
entries clone their member slices. It performs no network I/O, election,
durable-log write, or automatic topology publication; those remain caller and
Raft-transport responsibilities.

## Benchmark

Measured with `make benchmark-tt005-raft-configuration` on an AMD Ryzen 9
5950X, Go `amd64`, five samples per case.

| Operation | Time (5-run range) | Bytes/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Stable quorum evaluation, five voters | 283.4-286.5 ns | 80 | 1 |
| Joint quorum evaluation, five-to-six voters | 634.8-644.6 ns | 272 | 3 |
| Add-voter proposal plus apply | 1.246-1.271 us | 1,248 | 19 |

Joint evaluation is about 2.2x the stable check and uses 3.4x the bytes. This
is an explicit membership-change safety cost, not a default request-path cost;
stable configurations remain the normal state.

## Remaining Work

An actual Raft log/election implementation, peer transport, durable replay,
leadership fencing, and automatic topology/migration integration are not part
of this package slice.
