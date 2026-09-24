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
snapshot and leaves the transaction open; `Commit` keeps the accumulated state
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

```text
make test-mu031-retractable-aggregate
make test-mu031-package
make race-mu031-retractable-aggregate
make vet-mu031-retractable-aggregate
make verify-mu031-retractable-aggregate
```
