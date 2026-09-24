# TT-027 Validated Generated Columns

`hatSchema.NewValidatedMaterializedSource` adds an opt-in checked constructor
for `MaterializedSource`. `DerivedColumn.GeneratedDependencies` declares the
columns read by a generated callback. The constructor validates column names,
unknown dependencies, duplicate dependencies, evaluator presence, and cycles,
then caches a deterministic topological evaluation order.

```go
source, err := hatSchema.NewValidatedMaterializedSource([]hatSchema.DerivedColumn{
	{Name: "amount"},
	{
		Name:                 "tax",
		GeneratedDependencies: []string{"amount"},
		Generated: func(row hatSchema.Row) (interface{}, error) {
			return row["amount"].(int64) / 10, nil
		},
	},
})
if err != nil {
	// Invalid schemas fail before a source is published.
	}
_, _ = source.Insert(hatSchema.Row{"amount": int64(100)})
```

Generated callbacks receive a cloned row and are evaluated after defaults,
identity values, and sequences. The checked path evaluates generated columns in
dependency order even when declarations are out of order. `NewMaterializedSource`
continues to preserve the existing declaration-order behavior for compatibility.
The callbacks are Go functions, not parsed SQL expressions; SQL DDL integration
remains caller-owned.

## Benchmark

Command:

```text
make benchmark-tt027-generated-columns
```

Environment: Linux `amd64`, AMD Ryzen 9 5950X, Go `-benchmem -count=5`. The
materialization benchmark excludes constructor setup. The constructor rows
measure the one-time source creation and dependency validation cost.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Legacy declaration-order materialization | 704.5 | 1,032 | 8 | baseline |
| Validated dependency-order materialization | 697.7 | 1,032 | 8 | 1.01x faster; same bytes and allocations |
| Legacy constructor | 142.2 | 288 | 1 | baseline |
| Validated constructor | 777.0 | 1,144 | 13 | 5.46x slower; 3.97x bytes; 13x allocations |

The checked constructor is therefore appropriate when schema validation and
dependency correctness matter; it is not enabled by default and does not add
per-write memory overhead to legacy sources.

## Verification

```text
make format-tt027-generated-columns
make test-tt027-generated-columns
make benchmark-tt027-generated-columns
```

Focused tests cover reversed declarations, dependency evaluation, unknown and
duplicate dependencies, cycle rejection, and legacy compatibility.
