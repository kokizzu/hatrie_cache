# Differential GROUP BY

`hat/hatSql` provides `GroupCountDifferentialRows` and
`GroupSumInt64DifferentialRows` for exact generic COUNT and signed integer SUM
maintenance over differential row updates. The callback maps each input row to
a group identity. The identity becomes the `DifferentialRow.Key` of the
output, and the output `Row` contains one aggregate field: `count` or `sum` as
`int64`.

For every non-zero input update, the function computes the group count after
applying `Diff`:

- a group entering the result emits one `Diff: 1` row;
- an existing group emits `Diff: -1` for its previous count, then `Diff: 1`
  for its new count;
- a group reaching zero emits only the retraction;
- zero-diff inputs are ignored.

The output retraction and insertion use the input update's `Time`, and output
order follows input order. Output `Diff` is the relation weight (`-1` or `+1`);
the aggregate value itself is in `Row["count"]`. The operation starts with an
empty state for each call and does not mutate input rows.

`GroupSumInt64DifferentialRows` takes a second callback that extracts the
`int64` value contributed by each row. The row's `Diff` is multiplied by that
value and added to the group's sum. The function tracks multiplicity separately
from the sum, so a present group with sum zero still emits a valid aggregate
row. Retraction rows must carry the same value as the insertion they retract.
It emits the same retraction-then-insertion transition shape as COUNT, using
`Row["sum"]`, and rejects negative multiplicity, signed multiplication
overflow, and accumulator overflow without returning partial output.

```go
changes, err := hatSql.GroupSumInt64DifferentialRows(updates,
	func(row hatSql.SQLRow) string { return row["team"].(string) },
	func(row hatSql.SQLRow) (int64, error) { return row["points"].(int64), nil },
)
```

For two rows with values 3 and 4, the result is:

```text
red  +1 {sum: 3}
red  -1 {sum: 3}
red  +1 {sum: 7}
```

Classify sum failures with `errors.Is` against
`hatSql.ErrDifferentialGroupBySumOverflow`. A nil value callback returns
`hatSql.ErrDifferentialGroupByValueRequired`.

When both aggregates are needed, `GroupCountSumInt64DifferentialRows` performs
the same checked update in one pass and emits `Row["count"]` and `Row["sum"]`
together. This avoids maintaining two independent output streams while
preserving the same signed multiplicity and ownership rules.

```go
changes, err := hatSql.GroupCountSumInt64DifferentialRows(updates,
	func(row hatSql.SQLRow) string { return row["team"].(string) },
	func(row hatSql.SQLRow) (int64, error) { return row["points"].(int64), nil },
)
```

```go
updates := []hatSql.DifferentialRow{
	{Key: "one", Time: 1, Diff: 1, Row: hatSql.Row{"team": "red"}},
	{Key: "two", Time: 2, Diff: 1, Row: hatSql.Row{"team": "red"}},
}

changes, err := hatSql.GroupCountDifferentialRows(updates, func(row hatSql.SQLRow) string {
	return row["team"].(string)
})
```

The result is equivalent to these three differential rows:

```text
red  +1 {count: 1}
red  -1 {count: 1}
red  +1 {count: 2}
```

The function returns no partial output if a group's count would become
negative or overflow `int64`. Classify failures with `errors.Is` against
`hatSql.ErrDifferentialGroupByNegativeCount` or
`hatSql.ErrDifferentialGroupByCountOverflow`. A nil key callback returns
`hatSql.ErrDifferentialGroupByKeyRequired`.

## Differential grouped AVG

`GroupAverageInt64DifferentialRows` maintains signed `AVG(int64)` transitions
for callback-defined groups. It retains exact count and sum state internally,
then exposes `Row["avg"]` as `float64`. Weighted duplicates, negative
retractions, zero averages, and the retraction-then-insertion output shape are
preserved without rebuilding the input relation.

```go
changes, err := hatSql.GroupAverageInt64DifferentialRows(updates,
	func(row hatSql.SQLRow) string { return row["team"].(string) },
	func(row hatSql.SQLRow) (int64, error) { return row["points"].(int64), nil },
)
```

For values 3 and 4 in group `red`, the output is:

```text
red  +1 {avg: 3}
red  -1 {avg: 3}
red  +1 {avg: 3.5}
```

The function rejects negative multiplicity, signed value-times-diff
overflow, sum overflow, and callback errors without returning partial output.
It is a batch-scoped importable operator; SQL parser integration and automatic
planner selection remain unchanged.

## Differential grouped MIN/MAX

`GroupMinMaxInt64DifferentialRows` maintains `MIN` and `MAX` together for
callback-defined groups over signed `int64` differential updates. Its output
rows use `Row["min"]` and `Row["max"]` and follow the same retraction-then-
insertion convention as the other grouped differential aggregates.

```go
changes, err := hatSql.GroupMinMaxInt64DifferentialRows(updates,
	func(row hatSql.SQLRow) string { return row["team"].(string) },
	func(row hatSql.SQLRow) (int64, error) { return row["score"].(int64), nil },
)
```

Each group retains a multiplicity map keyed by value. Positive updates and
removals that do not eliminate the current endpoint are constant-time; a
removal of the current minimum or maximum scans that group's distinct values
to find the replacement. Duplicate weights are exact, negative group or
value multiplicities are rejected, and callback or overflow errors return no
partial output. Updates that do not change the visible minimum or maximum
produce no output.

The retained value map is the cost of supporting exact out-of-order
retractions. The API is batch-scoped and opt-in; existing COUNT and SUM paths
are unchanged.

## Differential grouped SUM(DISTINCT)

`GroupSumDistinctInt64DifferentialRows` maintains an exact signed differential
`SUM(DISTINCT value)` for callback-defined groups. It retains the total group
multiplicity plus a multiplicity count for each `int64` value. Adding a
duplicate or removing one of several duplicates does not change the visible
aggregate; entering or leaving a distinct value emits a retraction of the old
`Row["sum"]` followed by an insertion of the new sum.

```go
updates := []hatSql.DifferentialRow{
	{Key: "one", Time: 1, Diff: 1, Row: hatSql.Row{"team": "red", "points": int64(2)}},
	{Key: "two", Time: 2, Diff: 1, Row: hatSql.Row{"team": "red", "points": int64(2)}},
	{Key: "three", Time: 3, Diff: 1, Row: hatSql.Row{"team": "red", "points": int64(5)}},
}
changes, err := hatSql.GroupSumDistinctInt64DifferentialRows(updates,
	func(row hatSql.SQLRow) string { return row["team"].(string) },
	func(row hatSql.SQLRow) (int64, error) { return row["points"].(int64), nil },
)
```

The changes are equivalent to:

```text
red  +1 {sum: 2}
red  -1 {sum: 2}
red  +1 {sum: 7}
```

The operator preserves weighted duplicates, supports negative retractions,
rejects negative total or per-value multiplicity, and uses checked `int64`
addition including the `math.MinInt64` edge case. Callback failures and
overflow return no partial output. It is an importable batch-scoped primitive;
SQL planner wiring remains unchanged, so existing callers do not incur the
retained multiplicity-map cost unless they select this operator.

On the 5,120-update benchmark workload, maintaining the distinct sum directly
was `1.33x` faster than rebuilding each group's sum after every update, with
`1.01x` heap and `1.02x` allocations. The small retained per-value map is the
explicit cost of exact duplicate-preserving retractions.

## Differential grouped COUNT(DISTINCT)

`GroupCountDistinctInt64DifferentialRows` maintains an exact signed
differential `COUNT(DISTINCT value)` for callback-defined groups. It uses the
same total and per-value multiplicity rules as the distinct sum operator, but
emits `Row["count_distinct"]` and updates the visible count only when a value
enters or leaves the group. Duplicate weights therefore produce no output.

```go
updates := []hatSql.DifferentialRow{
	{Key: "one", Time: 1, Diff: 1, Row: hatSql.Row{"team": "red", "value": int64(2)}},
	{Key: "two", Time: 2, Diff: 1, Row: hatSql.Row{"team": "red", "value": int64(2)}},
	{Key: "three", Time: 3, Diff: 1, Row: hatSql.Row{"team": "red", "value": int64(5)}},
}
changes, err := hatSql.GroupCountDistinctInt64DifferentialRows(updates,
	func(row hatSql.SQLRow) string { return row["team"].(string) },
	func(row hatSql.SQLRow) (int64, error) { return row["value"].(int64), nil },
)
```

The changes are equivalent to:

```text
red  +1 {count_distinct: 1}
red  -1 {count_distinct: 1}
red  +1 {count_distinct: 2}
```

Negative total or per-value multiplicity, callback errors, and checked count
overflow return no partial output. The value map also handles `math.MinInt64`
as an ordinary key. The primitive is importable and opt-in; existing SQL
planner paths and typed-table behavior remain unchanged.

On the 5,120-update benchmark workload, incremental distinct-count
maintenance was `1.43x` faster than rebuilding each group's distinct set after
every update, with `1.01x` heap and `1.02x` allocations.

## Measured Cost

Benchmark command:

```text
make benchmark-sql-differential-group-by
```

The focused COUNT/SUM comparison is available through
`make benchmark-differential-sum-local-clean`.

On the development machine, 1,024 updates across 256 groups measured:

| Metric | Result |
| --- | ---: |
| Time | 310-323 us/op |
| Allocated memory | 853,331-853,333 B/op |
| Allocations | 3,592 allocs/op |

The benchmark includes the required aggregate transition rows and their
`count` maps. It uses a precomputed string group key in each input row and a
type-asserting callback, so key construction is excluded from the measurement.
