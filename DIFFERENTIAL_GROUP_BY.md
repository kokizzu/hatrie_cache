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
