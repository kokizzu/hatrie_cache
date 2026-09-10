# Rolling Schema Deployment

`hatSchema.NewRollingSchemaPlan` validates a conservative schema transition
and records the replicas that must move through it. `Begin` returns a
concurrency-safe deployment state machine:

```go
plan, err := hatSchema.NewRollingSchemaPlan(previous, next,
	[]string{"node-a", "node-b", "node-c"})
if err != nil {
	return err
}

deployment := plan.Begin()
for _, node := range plan.Nodes() {
	if err := installNextSchema(node, plan.NextSchema()); err != nil {
		return err
	}
	if err := deployment.Prepare(node); err != nil {
		return err
	}
	if err := activateNextSchema(node); err != nil {
		return err
	}
	if err := deployment.Activate(node); err != nil {
		return err
	}
}
if !deployment.Complete() {
	return errors.New("schema rollout is incomplete")
}
```

## Contract

The planner requires both schemas to be valid and to pass the existing
conservative rolling-compatibility rules. It allows version advancement,
nullable column additions, and relaxation of `NOT NULL`; it rejects type
changes, removals, reordering, required-column additions, and constraint
changes. Schema and node inputs are copied, normalized, and sorted.

Each node advances monotonically from `pending` to `prepared` to `active`.
Repeated `Prepare` and `Activate` calls are safe for retries. Skipped phases,
unknown nodes, duplicate nodes, and invalid plans return typed errors. Phase
reads are synchronized, and snapshots are returned in deterministic order.

The state machine is a control-plane coordination primitive. It does not
contact replicas, mutate schema storage, or relax the existing exact
replication contract. During a rollout, callers should use the existing
`ReplicationSchemaCompatibilityPolicy` to accept the validated previous
contract; after every node is active, callers can retire that previous
contract. The default replication behavior remains unchanged.

## Performance

The existing compatibility checker was measured before and after the feature
on the same five-run fixture:

| Path | Before | After | Allocation change |
|---|---:|---:|---:|
| `CheckRollingCompatibility` | 658.9 ns/op | 665.7 ns/op | 224 B/op, 3 allocs/op in both |
| `RollingSchemaDeployment.Phase` | not applicable | 13.77 ns/op | 0 B/op, 0 allocs/op |

The approximately 1.01x compatibility-check variation is within local benchmark
noise; the checker code and default request path are unchanged. The new phase
read is allocation-free and only runs on the explicit control-plane object.
