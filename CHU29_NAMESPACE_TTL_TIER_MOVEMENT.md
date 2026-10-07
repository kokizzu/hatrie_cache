# CH-U29 Namespace TTL Tier Movement

CH-U29 connects the existing age-based `hatStorage.StorageTierPolicy` and
`PlanStorageTierMoves` APIs to namespace lifecycle state. The bridge is
explicitly opt-in: a namespace with a nil `TierPolicy` keeps the previous
behavior and does not plan or execute tier movement.

## API

Set `NamespaceLifecyclePolicy.TierPolicy` when constructing the lifecycle
controller. The controller exposes:

```go
moves, err := controller.PlanNamespaceStorageTierMoves(
	ctx, "orders", parts,
)
report, err := controller.ExecuteNamespaceStorageTierMoves(
	ctx, "orders", parts, func(ctx context.Context, move hatStorage.StorageTierMove) error {
		// The caller copies, fsyncs, renames, and publishes placement metadata.
		return movePart(ctx, move)
	},
)
```

The planner preserves deterministic ordering and delegates age selection and
path selection to `StorageTierPolicy`. The executor preserves the existing
partial-progress report and cancellation behavior.

Before either operation, the controller evaluates the namespace TTL. An
expired namespace runs its configured archive/delete hook and refuses the tier
operation once it reaches a terminal state. Frozen, archived, and deleted
namespaces are rejected. An empty opt-in policy is rejected during controller
construction. The lifecycle read lock covers planning and execution so a
concurrent terminal transition cannot publish halfway through an operation.

## Defaults and ownership

The default remains off. Existing `SQLAdapterRegistry.Execute`, namespace
queries, and standalone storage-tier APIs are unchanged. The caller still
owns the physical copy, checksum, fsync, atomic rename, metadata persistence,
retry policy, and bandwidth scheduling.

## Measurement

The clean-base direct planner benchmark (five runs) measured **19.68-20.65
us/op, 35,368 B/op, 4 allocs/op**. After the bridge, direct planning measured
**19.46-20.42 us/op** with the same bytes and allocations. The new lifecycle
wrapper measured **20.03-22.06 us/op**, also with **35,368 B/op and 4
allocs/op**. The five-run means were approximately **19.81 us/op direct** and
**20.88 us/op through the namespace lifecycle**, or **+1.06 us / 5.4%** for
the opt-in expiry/state checks, with no additional allocation or retained
memory. The feature does not claim a faster planner; its benefit is lifecycle
correctness and safe TTL integration.

The benchmark command was:

```text
make codex-chu29-bench
```

Focused tests, race tests, and vet passed. The combined `hatStorage` and
`hatSql` check still reports the pre-existing M-U05 arrangement-recovery test
failures in `hat/hatSql`; it does not involve this feature.
