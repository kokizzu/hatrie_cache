# Summing Merge

`hatSql.SumSQLRows` provides an explicit SummingMergeTree-style merge
primitive. Rows with the same logical key are combined by adding only the
named numeric columns; the first row's other fields remain authoritative.

```go
merged, err := hatSql.SumSQLRows(rows,
    func(row hatSql.SQLRow) string { return row["id"].(string) },
    []string{"count", "amount"},
)
```

The result keeps keys in first-seen order and returns shallow copies of row
maps. Missing or nil summed values are ignored. Values must use the same
built-in numeric type within a key and column. Signed, unsigned, and floating
point overflow is rejected instead of silently wrapping; floating-point
infinity supplied as input remains valid, while a finite addition that becomes
infinite returns an overflow error.

The primitive supports `int`, all sized signed and unsigned integers, and
`float32`/`float64`. It does not infer which columns are measures and does not
alter source rows, storage parts, or durability state. Callers should choose
the key and measure columns from a schema or merge policy.

## Example

Input:

```text
(id=a, count=2, amount=1.5, label=first)
(id=b, count=4, amount=2.0, label=b)
(id=a, count=3, amount=2.5, label=second)
```

Output:

```text
(id=a, count=5, amount=4.0, label=first)
(id=b, count=4, amount=2.0, label=b)
```

Focused coverage is in `hat/hatSql/summing_merge_test.go`, including every
built-in numeric kind, missing values, stable ordering, input isolation,
overflow, type errors, invalid column lists, and an allocation-reporting
benchmark.
