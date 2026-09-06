# Differential DISTINCT

`hat/hatSql` provides `DistinctDifferentialRows` for incremental set
maintenance over signed row updates. It treats each `DifferentialRow.Key` as a
set member with a non-negative multiplicity:

- positive `Diff` increases multiplicity;
- negative `Diff` decreases multiplicity;
- zero `Diff` is ignored;
- `0 -> positive` emits one `Diff: 1` row;
- `positive -> 0` emits one `Diff: -1` row;
- changes while multiplicity stays positive emit nothing.

Updates are processed in input order. The emitted transition keeps the key and
timestamp from the update that crossed the boundary. Its `Row` map is a
shallow copy, so changing the emitted map does not mutate the input map. Values
inside the map are not deep-copied.

```go
updates := []hatSql.DifferentialRow{
	{Key: "alice", Time: 1, Diff: 2, Row: hatSql.Row{"name": "Alice"}},
	{Key: "alice", Time: 2, Diff: -1, Row: hatSql.Row{"name": "Alice"}},
	{Key: "alice", Time: 3, Diff: -1, Row: hatSql.Row{"name": "Alice"}},
}

transitions, err := hatSql.DistinctDifferentialRows(updates)
```

`transitions` contains the first update with `Diff: 1` and the last update
with `Diff: -1`; the middle update is suppressed. The function returns no
partial output when a key would become negative or when its multiplicity would
overflow `uint64`. Use `errors.Is` with
`hatSql.ErrDifferentialDistinctNegativeMultiplicity` or
`hatSql.ErrDifferentialDistinctOverflow` to classify those errors.

This function is deliberately batch-scoped: each call starts with an empty
state. A caller maintaining state across batches should retain multiplicities
by key and pass only the resulting signed updates to its own stream state.

## Measured Cost

Benchmark command:

```text
make benchmark-sql-differential-distinct
```

On the development machine, processing 1,024 updates over 256 keys measured:

| Metric | Result |
| --- | ---: |
| Time | 88-98 us/op |
| Allocated memory | 181,584-181,585 B/op |
| Allocations | 518 allocs/op |

The allocation profile includes the returned transition rows and cloned row
maps. The benchmark is a focused API measurement, not a comparison with a
remote database.
