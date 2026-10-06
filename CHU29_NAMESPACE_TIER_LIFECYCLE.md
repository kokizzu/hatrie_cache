# CH-U29 Namespace Tier Lifecycle

CH-U29 adds an opt-in bridge between persisted namespace lifecycle metadata and
the existing deterministic storage-tier planner. It does not start a mover,
perform filesystem I/O, or change the default storage path.

## API

Register a validated `StorageTierPolicy` once for each namespace:

```go
registry, err := hatStorage.NewStorageTierNamespaceRegistry(
	hatStorage.StorageTierNamespacePolicy{
		Namespace: "eu",
		Policy:    policy,
	},
)
if err != nil {
	return err
}
```

Each input part supplies its current tier and the persisted lifecycle timestamp.
The registry computes `now - LifecycleTime` and delegates to
`StorageTierPolicy.PlanStorageTierMoves`:

```go
moves, err := registry.Plan("eu", now, []hatStorage.StorageTierLifecyclePart{
	{
		Key:           "orders-0001",
		CurrentTier:   "hot",
		LifecycleTime: createdAt,
	},
})
```

`Execute` uses the same ordered, cancellable semantics as
`ExecuteStorageTierMoves`. The executor remains responsible for the actual
copy, fsync, atomic rename, and publication of placement metadata.

## Safety Contract

- Namespace names are trimmed, bounded to 256 bytes, valid UTF-8, and reject
  control characters.
- Empty or zero-value policies cannot be registered.
- Duplicate registration is rejected; an existing namespace policy cannot be
  silently replaced while a process is running.
- `now` and every `LifecycleTime` are required. Future lifecycle timestamps are
  rejected instead of producing a negative age.
- Registry lookup and registration are concurrency-safe. Policies are
  immutable after `NewStorageTierPolicy` returns.
- Planning has no I/O and no mutation. A caller must persist lifecycle metadata
  and retry failed moves using its own recovery protocol.

## Measured Cost

Five `-benchmem` samples were measured on an AMD Ryzen 9 5950X,
linux/amd64, for 64 parts with 62 resulting moves:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Existing direct `PlanStorageTierMoves` | 6,679 | 8,872 | 4 | baseline |
| CH-U29 namespace lifecycle `Plan` | 8,325 | 11,560 | 5 | 1.25x CPU, +30.3% bytes, +1 alloc |
| Existing single `StorageTierPolicy.Select` | 12.85 | 0 | 0 | unchanged control |

The adapter cost is caused by namespace lookup and constructing age-based
`StorageTierPart` values before calling the existing planner. It is paid only
by callers that opt into namespace lifecycle planning; the existing direct
planner and selection APIs remain unchanged.

Reproduce the focused verification and benchmark with:

```text
make codex-chu29-namespace-tier-focused
make codex-chu29-namespace-tier-race
make codex-chu29-namespace-tier-package
make codex-chu29-namespace-tier-package-race
make codex-chu29-namespace-tier-benchmark
```
