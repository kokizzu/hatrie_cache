# M-U35 Snapshot Blocking

`hatSql.SQLSnapshotReadinessRegistry` is an opt-in control-plane primitive for
source snapshot and hydration workflows. It gives a dependent a deterministic
readiness decision instead of making each caller poll several source objects
independently.

## Contract

1. Register each source object with `RegisterObject`. Objects start in
   `pending` state.
2. Register a dependent and its required object IDs with
   `RegisterDependent`.
3. Report monotone progress with `Advance`, then call `MarkReady` when the
   object is safe to read.
4. Call `Wait(ctx, dependent)` before admitting the dependent. It returns only
   after every required object is ready, or returns context cancellation,
   failure, or object cancellation.
5. Call `Snapshot(dependent)` for a detached, queryable status view. The
   generation covers every object status in that view, and `BlockedBy` is
   deterministic.

`Fail` records a bounded diagnostic code and permanently blocks affected
dependents. `Cancel` does the same with the `canceled` state. State transitions
are monotone: progress cannot regress and terminal objects cannot be reused.

## Example

```go
registry, err := hatSql.NewSQLSnapshotReadinessRegistry(
    hatSql.SQLSnapshotReadinessRegistryOptions{},
)
if err != nil {
    return err
}
for _, object := range []string{"orders", "customers"} {
    if err := registry.RegisterObject(object); err != nil {
        return err
    }
}
if err := registry.RegisterDependent(
    "dashboard",
    []string{"orders", "customers"},
); err != nil {
    return err
}

if err := registry.Advance("orders", 10); err != nil {
    return err
}
if err := registry.MarkReady("orders", 10); err != nil {
    return err
}
// The dashboard remains blocked until customers is ready too.
if err := registry.MarkReady("customers", 20); err != nil {
    return err
}
snapshot, err := registry.Wait(ctx, "dashboard")
if err != nil {
    return err
}
// snapshot.Ready is true and snapshot.Generation is a consistent read point.
_ = snapshot
```

## Defaults and boundaries

The zero-value options use bounded limits of 1,024 objects, 256 dependents,
and 64 dependencies per dependent. The registry does not automatically wire
itself into query execution, source connectors, or persistence; callers opt in
at the admission boundary. This keeps existing behavior and memory usage
unchanged when the feature is unused.

## Cost

Five benchmark samples on Linux/amd64 with an AMD Ryzen 9 5950X measured the
opt-in path after all objects were ready:

| Operation | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| `Snapshot`, 16 objects | 1,015 | 1,280 | 2 |
| `Snapshot`, 64 objects | 3,156 | 5,248 | 2 |
| `Wait`, already ready, 16 objects | 813 | 1,280 | 2 |

The feature is an admission/read-consistency improvement, not a claim of
faster query execution. The cost scales with the number of required objects
because the returned detached status is intentionally complete. There is no
cost on the ordinary SQL path unless a caller creates and consults a registry.
