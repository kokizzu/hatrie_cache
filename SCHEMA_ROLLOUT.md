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
## Sequential Coordinator

`RollingSchemaPlan.Run` adds an opt-in sequential coordinator around the
deployment state machine. The caller supplies the transport-specific hooks:

```go
deployment := plan.Begin()
err := plan.Run(
	ctx,
	deployment,
	func(ctx context.Context, node string, schema hatSchema.Schema) error {
		return installSchema(ctx, node, schema)
	},
	func(ctx context.Context, node string, schema hatSchema.Schema) error {
		return activateSchema(ctx, node, schema)
	},
)
```

Nodes are processed in the deterministic order returned by `plan.Nodes()`. A
successful install is recorded as `prepared` before activation is attempted.
If either hook fails, the completed phase remains recorded and a retry calls
only the unfinished hook. Cancellation behaves the same way. Each hook gets
an independent schema snapshot, and concurrent `Run` calls for the same node
are rejected while its transition is in progress.

The hooks remain responsible for HTTP/gRPC transport, remote authentication,
durability, and the actual schema install or activation. The coordinator is a
local control-plane helper and does not change the default replication or SQL
execution paths.

## Coordinator Cost

Linux/amd64, AMD Ryzen 9 5950X, five benchmark samples, `-benchmem`:

| Operation | Before | After | Heap / allocs after |
| --- | ---: | ---: | ---: |
| Four-node manual phase transitions | 0.388 us/op | 0.389 us/op | 404 B/op, 5/op |
| Four-node `RollingSchemaPlan.Run` with no-op hooks | n/a | 3.084 us/op | 7,124 B/op, 30/op |

The existing manual transition path has the same measured heap and allocation
profile after the change. The coordinator cost is opt-in and is dominated by
independent schema snapshots and callback dispatch; it is paid during schema
rollouts, not by cache commands or query execution.

## Durable Checkpoints

The opt-in `RollingSchemaPlan.Checkpoint` API makes the control-plane state
restartable without serializing schema bodies:

```go
checkpoint, err := plan.Checkpoint(deployment)
if err != nil {
	return err
}
wire, err := checkpoint.MarshalBinary()
if err != nil {
	return err
}

decoded, err := hatSchema.DecodeRollingSchemaCheckpoint(wire)
if err != nil {
	return err
}
deployment, err = plan.Restore(decoded)
```

The HRC1 frame contains both schema fingerprints and the deterministic node
phase list. Restore rejects a different schema transition, node set, order, or
phase, and the decoder bounds frames to 1 MiB and 65,536 nodes. A checkpoint
taken while an install or activation hook is running records the last stable
phase, so retrying after a process restart is safe. The Castagnoli CRC32
detects accidental truncation or corruption; it is not authentication, so
callers must use authenticated storage or transport when an attacker can
modify checkpoints. Persistence, fsync, and hook-side authentication remain
caller-owned and the default replication path is unchanged.

### Checkpoint Cost

Linux/amd64, AMD Ryzen 9 5950X, five benchmark samples, `-benchmem`; medians:

| Operation | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Manual four-node phase replay | 516.3 | 468 | 6 |
| In-memory checkpoint restore | 367.2 | 404 | 5 |
| HRC1 marshal | 160.6 | 120 | 2 |
| HRC1 unmarshal | 284.5 | 224 | 8 |
| Full unmarshal plus restore | 607.1 | 560 | 12 |

Compared with manual replay, restore-only is 0.71x the time, 0.86x the bytes,
and 0.83x the allocations. A full durable round trip is 1.18x the time, 1.20x
the bytes, and 2.00x the allocations; that cost is paid only when persisting
or recovering rollout state.
