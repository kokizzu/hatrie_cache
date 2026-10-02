# T-G16 Versioned Tuple Migration

This adds a Tarantool-inspired migration manager for the existing
`VersionedTuple` and `TupleFormat` primitives. It is opt-in: existing tuple
packing and storage paths do not call it, so there is no default hot-path
overhead.

## What It Provides

- Forward migration steps with strictly increasing schema versions.
- Source and destination format validation at every step.
- Optional preconditions before a step is allowed to run.
- Required rollback functions for every applied step.
- Reverse-order rollback when a later precondition or apply operation fails.
- The original tuple is returned with the error after a failed migration.
- Bounded, immutable plan metadata after `NewTupleMigrationPlan` returns.

## Example

```go
plan, err := NewTupleMigrationPlan("accounts", []TupleMigrationStep{
    {
        Name: "add-state",
        From: v1,
        To:   v2,
        Apply: func(tuple VersionedTuple, destination TupleFormat) (VersionedTuple, error) {
            return NewVersionedTuple(destination, []TupleFieldValue{
                TupleInt64(42), TupleString("apac"), TupleString("active"),
            })
        },
        Rollback: func(tuple VersionedTuple, source TupleFormat) (VersionedTuple, error) {
            return NewVersionedTuple(source, []TupleFieldValue{
                TupleInt64(42), TupleString("apac"),
            })
        },
    },
})
if err != nil {
    return err
}

migrated, err := plan.Migrate(tuple, v2.Version())
if err != nil {
    // migrated is the original tuple if a step had already been applied.
    return err
}
_ = migrated
```

Migration callbacks own the data transformation. They should be deterministic
and should not perform external side effects that cannot be rolled back. A
rollback error is joined with the original failure and is reported through
`ErrTupleMigrationRollback`.

## Measured Tradeoff

Command: `make round25-baseline-benchmark` and
`make round25-adoption-benchmark`, each using
`-benchtime=300ms -count=5 -benchmem`.

Machine: Linux/amd64, AMD Ryzen 9 5950X 16-Core Processor.

| Path | Samples (ns/op) | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| Baseline manual two-format packing | 599.6, 635.8, 605.0, 566.2, 605.3 | 605.0 | 896 | 6 |
| Adoption manual two-format packing | 611.5, 611.9, 587.4, 585.3, 556.2 | 587.4 | 896 | 6 |
| Adoption migration plan | 4496.0, 911.4, 901.4, 893.6, 895.4 | 901.4 | 896 | 6 |

The first migration-plan sample was a scheduling/warm-up outlier; the median
is used for comparison. The plan is about 1.49x the baseline manual CPU time
for this two-step workload, adding about 296 ns per tuple, while retaining the
same measured bytes and allocation count. That cost is paid only when a tuple
is explicitly migrated; normal reads, writes, and tuple packing remain
unchanged. The feature is therefore intended for controlled schema migration,
not for every-row request-path conversion.

## Verification

- `make round25-adoption-migration-test`
- `make round25-adoption-test`
- `make round25-adoption-race`
- `make round25-adoption-vet`

