# M-U31 Retractable Aggregate Capabilities

`hatSql.SQLAggregateState` already supports `Add`, `Merge`, and `Finalize`.
M-U31 adds opt-in capability contracts for aggregate states that can retract a
value or serialize a state snapshot. The original interface is unchanged, so
existing aggregate implementations remain source-compatible. It also adds an
opt-in transactional wrapper for serializable states that need rollback and
panic isolation around user callbacks.

## Retractable State

Implement `hatSql.SQLRetractableAggregateState` when removing a value is the
inverse of adding it for the values accepted by the aggregate:

```go
combinator, err := hatSql.NewSQLAggregateCombinator("sum", func() hatSql.SQLAggregateState {
    return newSumState()
})
if err != nil {
    return err
}

registry := hatSql.NewSQLAggregateCombinatorRegistry()
if err := registry.Register(combinator); err != nil {
    return err
}

state, err := registry.NewRetractableState("sum")
if err != nil {
    return err
}
if err := state.Add(int64(10)); err != nil {
    return err
}
if err := state.Retract(int64(10)); err != nil {
    return err
}
```

`NewRetractableState` constructs a fresh state and verifies the capability with
a type assertion. A legacy aggregate returns
`ErrSQLAggregateCombinatorNotRetractable`; it is never silently recomputed.
The direct `SQLAggregateCombinator.NewRetractableState` method provides the
same check without a registry.

## Serializable State

Implement `hatSql.SQLSerializableAggregateState` when the state owns a stable
binary snapshot representation:

```go
state, err := registry.NewSerializableState("sum")
if err != nil {
    return err
}
snapshot, err := state.MarshalBinary()
if err != nil {
    return err
}

restored, err := registry.NewSerializableState("sum")
if err != nil {
    return err
}
if err := restored.UnmarshalBinary(snapshot); err != nil {
    return err
}
```

The bytes are implementation-owned. Callers that persist or transfer them
must provide their own version, schema, checksum, size, and compatibility
policy. `ErrSQLAggregateCombinatorNotSerializable` is returned when a
registered factory exposes only the base contract. The direct combinator
method is also available.

## Transactional Mutations

Use `SQLAggregateTransaction` when a caller needs one atomic mutation boundary
around `Add`, `Retract`, and `Merge`:

```go
transaction, err := hatSql.NewSQLAggregateTransaction(state)
if err != nil {
    return err
}
if err := transaction.Add(int64(10)); err != nil {
    _ = transaction.Rollback()
    return err
}
if err := transaction.Commit(); err != nil {
    return err
}
```

The constructor requires `SQLSerializableAggregateState` and captures one
defensive binary snapshot. A callback error or panic restores that initial
snapshot and leaves the transaction open only if restoration succeeds. If
restoration returns an error or panics, the transaction closes and rejects
`Add`, `Retract`, `Merge`, `Finalize`, `Commit`, and `Rollback`. The caller must
discard the underlying state because it may be partially mutated. The returned
error preserves both the mutation and restoration errors through `errors.Is`.
`Commit` keeps the accumulated state
and `Rollback` restores the initial state and closes the transaction.
`ErrSQLAggregateCallbackPanic` converts callback and snapshot panics into
errors. The wrapper is single-owner and does not add synchronization.

This is intentionally a caller-owned boundary: aggregate combinators and SQL
planning do not create transactions automatically. A transaction retains the
snapshot bytes until commit or rollback, so its memory cost scales with the
serialized aggregate state.

## Compatibility And Safety

- `NewState` continues to construct every valid legacy aggregate state.
- Capability discovery is explicit and opt-in; it does not alter ordinary
  aggregate execution or enable a planner fast path automatically.
- The capability interfaces alone do not promise rollback after a callback
  error or panic. Use `SQLAggregateTransaction` when exact rollback is needed;
  it requires the serializable capability and restores the transaction's
  initial snapshot.
- Retraction and serialization do not add locks. The state keeps the same
  ownership and concurrency requirements as the base aggregate contract.
- Capability discovery constructs one state before checking its methods. The
  benchmark below measures that constructor/type-assertion cost, not the
  per-row `Add`, `Retract`, `Merge`, or `Finalize` path.

This is a small importable building block for Materialize-style differential
maintenance and ClickHouse-style mergeable state transfer. Automatic SQL
planner integration and distributed checkpoint orchestration remain separate
contracts.

## Measurement

Commands:

```text
make benchmark-mu031-retractable-aggregate-baseline
make benchmark-mu031-retractable-aggregate
```

Both commands run five one-second samples on Linux/amd64 with an AMD Ryzen 9
5950X. The baseline calls `NewState` on the same retractable or serializable
factory; the capability path adds only the opt-in type assertion.

| Capability | Baseline median | Capability median | CPU delta | Baseline memory | Capability memory |
| --- | ---: | ---: | ---: | ---: | ---: |
| Retraction | 113.6 ns/op | 128.8 ns/op | 1.13x, 13.4% higher | 24 B/op, 2 allocs | 24 B/op, 2 allocs |
| Binary serialization | 107.8 ns/op | 119.5 ns/op | 1.11x, 10.9% higher | 24 B/op, 2 allocs | 24 B/op, 2 allocs |

The measured tradeoff is limited to constructor-time capability discovery:
there is no additional retained or transient memory in this fixture. The
feature is worthwhile when a caller needs correctness-preserving discovery of
incremental or checkpoint-capable states; callers that already know the
concrete type can keep using `NewState` and a direct type assertion.

The transactional wrapper has a separate cost profile. On the same host, a
direct `Add` measured 0.2642 ns/op, while transactional `Add` measured 8.618
ns/op with zero per-operation bytes and allocations. Creating the transaction
measured 108.9 ns/op, 88 B/op, and 4 allocs/op for an 8-byte snapshot. Use it
for correctness boundaries, not for every row when callbacks are already
trusted and rollback is unnecessary. See the raw samples in the benchmark
section below.

Raw samples are recorded in
[BENCHMARK.md](BENCHMARK.md#mu-031-retractable-aggregate-capabilities).

## Verification

### Failed restoration regression (2026-10-07)

Baseline `f4039d3d` contains the transaction implementation from `c5f3bd97`.
The new `TestMU031AggregateTransactionFailedRecoveryClosesBoundary` fails on
that baseline for Add/Retract/Merge with both an erroring and a panicking
restore callback: the restoration error is not discoverable through
`errors.Is`, and `Commit` succeeds after failed recovery. The fix closes only
this failed-recovery path and preserves both error causes. Existing successful
rollback/reuse behavior continues to pass.

Verification via the recovery checkout's Makefile targets:

- `make codex-mu031-recovery-test`: failed before the fix, passed after it.
- `make codex-mu031-recovery-package`: full `hat/hatSql` package passed.
- `make codex-mu031-recovery-race`: transaction regressions passed under race.
- `make codex-mu031-recovery-vet`: SQL package passed.

Published equivalents are `make test-mu031-m043`,
`make test-mu031-package-m043`, `make race-mu031-m043` (broader full-package
race coverage), and `make vet-mu031-m043`. Other packages and the broader
published race target were not rerun for this isolated transaction change.

`make codex-mu031-recovery-compare` ran the existing DirectAdd,
TransactionAdd, and TransactionCreate benchmarks sequentially in separate
baseline/candidate worktrees, five samples each at `-benchtime=200ms -cpu=1`
with `-benchmem`. Host: Linux/amd64, Ryzen 9 5950X, Go 1.26.6.
An earlier candidate run overlapped compilation and is excluded from this
comparison. No speedup is claimed; this is a correctness fix.

| Path | Baseline median ns/op | Fixed median ns/op | B/op (both) | allocs/op (both) |
| --- | ---: | ---: | ---: | ---: |
| Direct Add control | 0.4779 | 0.4842 | 0 | 0 |
| Transaction Add | 8.214 | 8.232 | 0 | 0 |
| Transaction construction | 98.86 | 90.43 | 88 | 4 |

Raw successful transaction Add samples (ns/op): baseline
`8.309, 8.230, 8.200, 8.214, 8.200`; fixed
`8.427, 8.232, 8.339, 7.864, 7.806`.
Construction samples: baseline `91.34, 92.47, 101.7, 98.86, 98.97`;
fixed `89.27, 89.47, 90.43, 128.0, 93.15`.
The success-path allocation counts are unchanged and Add medians differ by
0.2%. Failure now retains both causes in the error chain and releases the
snapshot; callers cannot commit or reuse a transaction whose recovery failed.

### Original capability checks

```text
make test-mu031-retractable-aggregate
make test-mu031-package
make race-mu031-retractable-aggregate
make vet-mu031-retractable-aggregate
make verify-mu031-retractable-aggregate
```
