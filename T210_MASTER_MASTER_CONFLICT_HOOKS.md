# T210 Master-Master Conflict Hooks

`hatReplication.ConflictPolicy.Hook` adds an opt-in callback for conflict
resolution. It is intended for master-master replication policies that need
application-specific decisions from the source identity and source sequence,
while keeping the existing deterministic policy as the default.

```go
registry, err := hatReplication.NewConflictPolicyRegistry(
	hatReplication.ConflictPolicy{
		Mode: hatReplication.ConflictPolicyLastWriteWins,
		Hook: func(context hatReplication.ConflictHookContext) (
				hatReplication.ConflictHookDecision, error,
		) {
			if context.Left.NodeID == "region-a" && context.Left.Sequence < context.Right.Sequence {
				return hatReplication.ConflictHookUseLeft, nil
			}
			return hatReplication.ConflictHookUsePolicy, nil
		},
	},
)
if err != nil {
	return err
}

winner, err := registry.Resolve("orders", localVersion, incomingVersion)
```

The callback receives the normalized space and both `ConflictVersion` values.
`NodeID` is the source identity and `Sequence` is that source's write
sequence. The callback can return `ConflictHookUseLeft`, `UseRight`,
`UsePolicy`, or `Reject`; callback errors are returned unchanged.

Hooks run only after both versions pass validation and only for distinct
versions. Equal versions preserve the existing value without invoking the
callback. No raw keys or values are exposed, and callbacks are synchronous,
so a production hook should be deterministic, bounded, and free of blocking
network calls. The nil hook remains the default and retains the existing
last-write-wins/source-priority/reject behavior.

This is a decision boundary, not automatic replication transport. Callers
still own ordering, persistence, idempotency, and any redacted conflict
introspection they need.
