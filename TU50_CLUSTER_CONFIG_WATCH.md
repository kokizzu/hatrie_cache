# T-U50 Cluster Configuration Watch

`hatTopology.ConfigWatchLog` already provides a bounded authenticated event
log for local configuration. This increment adds the cross-node apply
contract needed by a peer transport:

```go
applied, err := log.ApplyReplicated(ctx, "peer-a", hatTopology.ConfigWatchEvent{
	Version: 42,
	Source:  "node-a",
	Key:     "feature/cache",
	Value:   []byte("on"),
})
```

The method requires an explicit globally ordered version. Applying the exact
retained event again is an idempotent success with `applied == false`. A
same-version event with different source, key, value, or delete state returns
`ErrConfigWatchReplicationConflict`. A version that skips the next expected
event returns `ErrConfigWatchVersionGap`, so a reconnecting transport must
replay the missing range first. Every replication call goes through the
configured authorizer as `ConfigWatchReplicate`.

The API does not start a worker, open a socket, or invent membership and
consensus. The peer/consensus layer owns transport authentication, global
version assignment, snapshot recovery after `ErrConfigWatchHistoryGap`, and
fan-out. Existing local `Publish`, `Read`, and `Wait` behavior is unchanged.

## Verification

```sh
make goal-round87-tu50-baseline
make goal-round87-tu50-baseline-bench
make goal-round87-tu50-focused
make goal-round87-tu50-verify
make goal-round87-tu50-bench
```

The focused tests cover first apply, exact replay, conflicting replay, version
gaps, explicit-version validation, authorization action, and resumed reads.
