# TT-016 Versioned Tuple Migrations

`hatDataStructure.VersionedTuple` records the `TupleFormat` version attached
to one packed tuple. `VersionedTupleMigrationManager` adds the missing
control-plane piece: register immutable formats, register one deterministic
forward step per source version, inspect the exact plan, and migrate one tuple
atomically to the current or a selected target version.

## Example

```go
manager, err := hatDataStructure.NewVersionedTupleMigrationManager(formatV3)
if err != nil {
    return err
}
if err := manager.RegisterFormat(formatV1); err != nil {
    return err
}
if err := manager.RegisterFormat(formatV2); err != nil {
    return err
}
if err := manager.RegisterMigration(1, 2, func(values []hatDataStructure.TupleFieldValue) ([]hatDataStructure.TupleFieldValue, error) {
    return []hatDataStructure.TupleFieldValue{
        values[0],
        hatDataStructure.TupleString(values[1].String + "-migrated"),
    }, nil
}); err != nil {
    return err
}
if err := manager.RegisterMigration(2, 3, func(values []hatDataStructure.TupleFieldValue) ([]hatDataStructure.TupleFieldValue, error) {
    return append(values, hatDataStructure.TupleString("ap-southeast")), nil
}); err != nil {
    return err
}

plan, err := manager.Plan(1, 3) // plan.Versions is [1, 2, 3]
if err != nil {
    return err
}
current, err := manager.Migrate(oldTuple)
```

Each migration callback receives values unpacked and copied from the source
format. The destination `TupleFormat` repacks and validates the returned
values, applying its defaults or generated fields for omitted trailing
fields. `MigrateTo` is available when a caller needs an intermediate target.

## Guarantees

- Unknown formats, missing paths, duplicate steps, cycles, callback errors,
  source validation failures, and destination type errors fail before a result
  is published.
- The input `VersionedTuple` is never mutated. A failed migration returns no
  partial tuple, so a caller can keep the old record and retry or quarantine
  it.
- One outgoing step per source version makes plans deterministic and exposes
  accidental branching during registration.
- The manager is thread-safe for registration, planning, and migration. A
  migration snapshots its plan before calling user callbacks and does not hold
  the manager lock while executing them.
- No goroutine, storage rewrite, journal entry, or automatic schema cutover is
  started. The caller persists or publishes the returned tuple explicitly.
- Format and tuple size limits remain those enforced by `TupleFormat` and
  `VersionedTuple`; callbacks are trusted Go code and should enforce any
  domain-specific limits before returning values.

## Cost And Scope

This is an operational correctness feature, not a hot-path optimization.
Ordinary tuple packing, reads, and field updates do not construct a migration
manager and therefore keep their existing cost. Migration itself necessarily
unpacks and repacks each schema boundary.

The matched three-version fixture measured the following medians on an AMD
Ryzen 9 5950X:

| Path | Time | Allocated bytes | Allocations | Relative to direct path |
| --- | ---: | ---: | ---: | ---: |
| Direct hand-written unpack/transform/repack | 1.42 us/op | 2,232 B/op | 12 | 1.00x |
| Migration manager | 1.73 us/op | 2,688 B/op | 17 | 1.22x time, 1.20x bytes, 1.42x allocs |

The manager cost is the explicit price for format-boundary validation,
deterministic path checks, cycle protection, and atomic failure semantics. It
should be used at migration, restore, or rollout boundaries rather than for
every ordinary tuple read. Reproduce with `make benchmark-tg16`.
