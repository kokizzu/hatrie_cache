# C154g Replication Schema Rollout Contract Bridge

This opt-in `hatCache.ReplicationSchemaRollout` composes the existing
`hatSchema.RollingSchemaPlan` with replication schema contracts. It closes the
gap between replica phase state and the compatibility policy used by HTTP and
gRPC replication callers.

```go
rollout, err := hatCache.NewReplicationSchemaRollout(previous, next,
	[]string{"node-a", "node-b"})
if err != nil {
	return err
}

if err := rollout.Run(ctx, installSchema, activateSchema); err != nil {
	return err
}
if rollout.Complete() {
	policy, err := rollout.CompatibilityPolicy()
	if err != nil {
		return err
	}
	// Install policy in the caller-owned command/stream options.
	_ = policy
}
```

Before the last replica activates, `Accepts` allows the validated previous and
next contracts. `Contract(node)` reports the previous contract for pending,
prepared, and in-progress nodes, and the next contract for active nodes. Once
all nodes are active, the previous contract is rejected and a refreshed
`CompatibilityPolicy` is strict to the next contract. Unknown nodes, invalid
schema transitions, activation before preparation, and nil receivers fail
closed. `Begin` reuses the immutable validated plan for another rollout.

The API does not contact replicas, publish topology, persist checkpoints, or
change default replication. Transport, authentication, durable checkpointing,
and installation/activation remain caller-owned. `Accepts` is allocation-free;
the policy refresh is an explicit control-plane operation after completion.

## Measurement

Linux/amd64, AMD Ryzen 9 5950X, five samples, four replicas, no-op hooks,
`-benchtime=200ms -benchmem`:

| Path | Median ns/op | B/op | Allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Existing `RollingSchemaPlan.Run` | 4,825 | 7,636 | 30 | 1.00x |
| `ReplicationSchemaRollout.Run` | 4,934 | 7,636 | 30 | 1.02x time |

The bridge adds about 2.3% control-plane time in this fixture and no measured
heap or allocation increase. The cost is paid only when a caller explicitly
creates or runs a schema rollout; ordinary reads, writes, and replication
continue using their existing paths.

Raw samples:

```text
manual-plan: 5151, 4937, 4825, 4653, 4672 ns/op; 7636 B/op; 30 allocs/op
replication-rollout: 5009, 4957, 4756, 4823, 4934 ns/op; 7636 B/op; 30 allocs/op
```
