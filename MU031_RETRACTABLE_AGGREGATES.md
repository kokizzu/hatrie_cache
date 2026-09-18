# M-U31 Retractable Aggregate Capabilities

`hatSql.SQLAggregateState` already supports `Add`, `Merge`, and `Finalize`.
M-U31 adds opt-in capability contracts for aggregate states that can retract a
value or serialize a state snapshot. The original interface is unchanged, so
existing aggregate implementations remain source-compatible.

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

## Compatibility And Safety

- `NewState` continues to construct every valid legacy aggregate state.
- Capability discovery is explicit and opt-in; it does not alter ordinary
  aggregate execution or enable a planner fast path automatically.
- The capability interfaces do not promise rollback after a callback error or
  panic. Aggregate callbacks should validate before mutation and callers should
  discard a state after an implementation-defined failure unless its contract
  says it is reusable.
- Retraction and serialization do not add locks. The state keeps the same
  ownership and concurrency requirements as the base aggregate contract.
- Capability discovery constructs one state before checking its methods. The
  benchmark below measures that constructor/type-assertion cost, not the
  per-row `Add`, `Retract`, `Merge`, or `Finalize` path.

This is a small importable building block for Materialize-style differential
maintenance and ClickHouse-style mergeable state transfer. Automatic SQL
planner integration, distributed checkpoint orchestration, and transactional
rollback remain separate contracts.

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
