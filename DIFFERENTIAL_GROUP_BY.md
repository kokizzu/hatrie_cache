# Differential GROUP BY

`hat/hatSql` provides `GroupCountDifferentialRows` for exact generic COUNT
maintenance over signed row updates. The callback maps each input row to a
group identity. The identity becomes the `DifferentialRow.Key` of the output,
and the output `Row` contains one field: `count` as `int64`.

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

## Measured Cost

Benchmark command:

```text
make benchmark-sql-differential-group-by
```

On the development machine, 1,024 updates across 256 groups measured:

| Metric | Result |
| --- | ---: |
| Time | 310-323 us/op |
| Allocated memory | 853,331-853,333 B/op |
| Allocations | 3,592 allocs/op |

The benchmark includes the required aggregate transition rows and their
`count` maps. It uses a precomputed string group key in each input row and a
type-asserting callback, so key construction is excluded from the measurement.
