# T210: Master-Master Conflict Hooks

`hatReplication.ConflictPolicyRegistry` now accepts an optional
`ConflictHook`. The hook runs only when two valid `ConflictVersion` values are
different, so equal or replayed versions preserve the existing deterministic
path.

```go
registry, err := hatReplication.NewConflictPolicyRegistry(
	hatReplication.ConflictPolicy{
		Mode: hatReplication.ConflictPolicyLastWriteWins,
		Hook: func(context hatReplication.ConflictHookContext) (hatReplication.ConflictHookDecision, error) {
			// Local.NodeID/Sequence and Remote.NodeID/Sequence identify both writers.
		if context.Local.NodeID == "region-a" {
			return hatReplication.ConflictHookKeepLocal, nil
		}
		return hatReplication.ConflictHookAcceptRemote, nil
		},
	},
)
if err != nil {
	panic(err)
}

winner, err := registry.ResolveWithContext(hatReplication.ConflictHookContext{
	Space:     "orders",
	KeyDigest: sha256.Sum256([]byte("orders:42")),
	Local:     hatReplication.ConflictVersion{NodeID: "region-a", Sequence: 7},
	Remote:    hatReplication.ConflictVersion{NodeID: "region-b", Sequence: 44},
})
```

`ConflictHookDecision` values are:

- `ConflictHookUsePolicy`: continue with last-write-wins, source priority, or
  reject policy.
- `ConflictHookKeepLocal`: retain the local version.
- `ConflictHookAcceptRemote`: select the remote version.
- `ConflictHookReject`: return `ErrConflictHookRejected`.

The hook receives a copied context. `KeyDigest` is caller-supplied and is zero
when the simpler `Resolve(space, left, right)` method is used. Raw keys and
values are not retained or passed by the hook. Use a keyed digest when the key
itself is sensitive.

The feature is opt-in. A policy without a hook keeps the prior resolver path,
with zero allocations in the benchmark. Hook errors are returned unchanged so
callers can use `errors.Is` with their own sentinel errors. Invalid hook
decisions return `ErrConflictHookDecision`.

This is a conflict-resolution callback, not consensus or replication transport:
the caller remains responsible for applying the selected version, persisting
the decision, and retrying or quarantining rejected writes.
