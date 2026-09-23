# T204 Supervised Failover

T204 adds an explicit operator control plane for leader failover. The normal
automatic election behavior remains the default. Operators can pin a healthy
replica, keep that decision pinned while recovery is checked, and explicitly
release the shard back to topology election.

## Lifecycle

```go
if err := store.SetLeaderOverride(0, "node-b", "primary maintenance"); err != nil {
	return err
}
if err := store.BeginRecovery(0, "primary restored"); err != nil {
	return err
}
if err := store.CompleteRecovery(0); err != nil {
	return err
}
```

`SetLeaderOverride` requires a non-empty reason, a currently registered shard
owner, and a healthy node. Health includes topology membership, maintenance
state, explicit offline state, and heartbeat requirements. Repeating the call
is an explicit replacement of the prior operator decision.

While the control exists, `LeaderForKey` does not automatically promote a
different candidate. If the controlled node becomes unhealthy, the route is
reported unavailable instead of silently failing over. This keeps an operator
decision from turning into an unreviewed chain of promotions.

`BeginRecovery` changes the control state from `operator_override` to
`recovery` while keeping the selected leader pinned. `CompleteRecovery` only
succeeds when the controlled leader is healthy, then removes the control and
returns the shard to automatic election. The current state is visible through
`ElectionStatus.Controls` and `ElectionStore.Controls`.

The root `hatCache` package re-exports the state and control types and methods;
the implementation is in `hatTopology`.

## Safety Boundary

Controls are in-memory state. The operator or supervisory control plane must
persist the decision, replay it after restart, and audit who issued it. T204
does not provide consensus, durable failover records, or cross-node lock
acquisition. Pair it with topology publication and T203 fencing when stale
writers must be rejected.

## Verification

```text
make test-t204
make test-t204-package
make compile-t204-root
make race-t204
make vet-t204
make benchmark-t204
```
