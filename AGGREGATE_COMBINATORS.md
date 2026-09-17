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

## Built-In SQL State And Merge

SQL also provides opt-in built-in state combinators for transferring partial
aggregate work between query workers or materialized views:

```sql
FROM events
SELECT region, SUM_STATE(amount) AS amount_state
GROUP BY region
```

The matching merge function consumes the returned `[]byte` values:

```sql
FROM VALUES ($1), ($2) AS partial(state)
SELECT SUM_MERGE(partial.state) AS amount
```

The supported pairs are `COUNT_STATE`/`COUNT_MERGE`,
`SUM_STATE`/`SUM_MERGE`, `AVG_STATE`/`AVG_MERGE`, `MIN_STATE`/`MIN_MERGE`,
and `MAX_STATE`/`MAX_MERGE`. `COUNT_STATE()` and `COUNT_STATE(*)` count every
filtered row; `COUNT_STATE(expr)` ignores NULL values. The numeric functions
ignore NULL and non-numeric values using the same `sqlNumber` rule as the
ordinary aggregates. Empty numeric states merge to NULL, while an empty count
state merges to zero.

The SQL state is a compact `HAST` version-1 binary envelope. It is strict about
the marker, version, kind, flags, count, payload length, and trailing bytes, so
malformed or wrong-kind input returns an error instead of being interpreted as
another aggregate. This SQL envelope is separate from the caller-owned
`SQLAggregateCombinator` registry contract above.

Existing aggregate syntax, commands, journal/storage formats, and defaults are
unchanged. The feature is useful when a producer can send one partial state
instead of every input value; it does not make ordinary aggregate execution
automatically distributed.

## OrNull aggregates

The ClickHouse-style `OrNull` forms are available for the built-in scalar
aggregates:

```sql
FROM events
SELECT COUNT_OR_NULL(value), SUM_OR_NULL(value), AVG_OR_NULL(value),
       MIN_OR_NULL(value), MAX_OR_NULL(value)
```

`COUNT_OR_NULL(*)` returns the filtered row count, but returns `NULL` when no
row contributes. `COUNT_OR_NULL(value)` counts non-NULL values and returns
`NULL` when that count is zero. The numeric forms ignore NULL and non-numeric
values using the ordinary aggregate `sqlNumber` rule and return `NULL` when no
numeric value contributes. `FILTER (WHERE ...)` is supported. These forms use
the existing bounded global streaming aggregate path when the query qualifies;
ordinary aggregate names and their empty-result behavior are unchanged.

## Arg-extreme states

`ARGMAX_STATE(argument, ordering)` and `ARGMIN_STATE(argument, ordering)` retain
the argument associated with the largest or smallest ordering value. The
matching `ARGMAX_MERGE(state)` and `ARGMIN_MERGE(state)` functions combine those
partial winners:

```sql
FROM events
SELECT region, ARGMAX_STATE(payload, score) AS latest_state
GROUP BY region
```

```sql
FROM VALUES ($1), ($2) AS partial(state)
SELECT ARGMAX_MERGE(partial.state) AS payload
```

Rows with a NULL argument or ordering value are ignored. Equal ordering values
keep the first winner, including when partial states are merged. String
ordering follows the query collation, and the collation is stored in the state;
merging non-empty states with different collations is rejected. Empty states
are valid and merge to NULL.

The `HAEX` version-1 binary state preserves supported scalar types, including
integers, unsigned integers, floats, strings, bytes, booleans, timestamps,
`DATE`, `DECIMAL`, `UUID`, `DURATION`, IPv4, and IPv6. It has bounded length
decoding, strict magic/version/kind/flag checks, and rejects trailing bytes or
wrong-kind states. State creation adds serialization work; the ordinary
`ARGMAX`/`ARGMIN` path remains unchanged.
