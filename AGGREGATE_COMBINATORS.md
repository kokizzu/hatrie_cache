# Aggregate Combinators

`hatSql.SQLAggregateState` is the reusable partial-aggregate contract used by
workers and merge stages:

```go
type SQLAggregateState interface {
    Add(value interface{}) error
    Merge(other SQLAggregateState) error
    Finalize() (interface{}, error)
}
```

Register a factory when the same aggregate needs independent worker-local
states and a deterministic merge boundary:

```go
combinator, err := hatSql.NewSQLAggregateCombinator("sum", func() hatSql.SQLAggregateState {
    return &sumState{}
})
registry := hatSql.NewSQLAggregateCombinatorRegistry()
_ = registry.Register(combinator)

left, _ := registry.NewState("sum")
right, _ := registry.NewState("sum")
_ = left.Add(int64(2))
_ = right.Add(int64(3))
_ = left.Merge(right)
result, err := left.Finalize()
```

Names are trimmed, case-insensitive, and stored in uppercase. Factories must
return a non-nil independent state. The registry rejects invalid and duplicate
definitions, returns a fresh state for every lookup, and reports names in
sorted order. Registration and lookup are safe concurrently; the aggregate
state itself is owned by its caller and must provide its own synchronization if
shared between workers.

The merge contract is explicit: `Add` incorporates one input value, `Merge`
combines another partial state of the same aggregate, and `Finalize` produces
the user-visible result only after all partial states have been merged. Type
checking, overflow behavior, and serialization of the state belong to the
registered implementation.

This package-level registry does not silently replace the built-in SQL
aggregates and does not persist state. It is a low-level extension boundary for
parallel aggregation, materialized views, or caller-owned execution plans.

Focused coverage is in `hat/hatSql/aggregate_combinator_test.go`, including
state merge/finalize, invalid and duplicate definitions, sorted names, and
concurrent lookups.
