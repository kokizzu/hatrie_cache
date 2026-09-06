# Aggregate Combinators

`hatSql.SQLAggregateState` is the state/merge/finalize boundary used by
incremental and distributed aggregates:

```go
type SQLAggregateState interface {
	Add(value interface{}) error
	Merge(other SQLAggregateState) error
	Finalize() (interface{}, error)
}
```

Register a factory under a stable name and create independent states for
workers or partitions:

```go
sum, err := hatSql.NewSQLAggregateCombinator("sum", newSumState)
if err != nil {
	return err
}
registry := hatSql.NewSQLAggregateCombinatorRegistry()
if err := registry.Register(sum); err != nil {
	return err
}

left, _ := registry.NewState("SUM")
right, _ := registry.NewState("sum")
// Add values to each worker state.
if err := left.Merge(right); err != nil {
	return err
}
result, err := left.Finalize()
```

Names are trimmed and normalized to uppercase. Registries are safe for
concurrent registration and lookup; returned states are independent, but an
individual state must be owned by one worker at a time. Duplicate names,
missing names, empty definitions, and factories returning nil states are
rejected.

This is an additive execution primitive inspired by ClickHouse aggregate
states and Materialize differential partial aggregation. It does not change
existing SQL parsing or aggregate behavior. A planner or distributed worker
can opt into partial aggregation while preserving the existing final result
contract.
