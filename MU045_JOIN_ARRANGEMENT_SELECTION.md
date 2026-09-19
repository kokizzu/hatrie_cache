# M-U45 Incremental Join Arrangement Selection

M-U45 adds an opt-in selector for changing join predicates. It compares
explicit alternatives for one left/right table orientation and returns the
lowest-cost exact arrangement plan without mutating a live arrangement
registry.

## Usage

```go
selection, err := hatSql.SelectTypedTableJoinArrangement(
	catalog,
	hatSql.TypedTableJoinArrangementSelectionRequest{
		Alternatives: []hatSql.TypedTableJoinArrangementRequest{
			{
				LeftTableName:      "orders",
				RightTableName:     "customers",
				Definition:         hatSql.TypedTableJoinDefinition{LeftField: "customer_id", RightField: "id"},
				EstimatedStateRows: 2_000,
			},
			{
				LeftTableName:      "orders",
				RightTableName:     "customers",
				Definition:         hatSql.TypedTableJoinDefinition{LeftField: "account_id", RightField: "id"},
				EstimatedStateRows: 500,
			},
		},
	},
	hatSql.DefaultTypedTableArrangementAdvisorOptions(),
)
if err != nil {
	return err
}

// The selected request can then be passed to the existing arrangement
// registry. Selection itself does not acquire, hydrate, or release state.
chosen := selection.Selected.Request
_ = chosen
```

The selector rejects an empty alternative list, duplicate exact predicates,
and candidates with different left/right table orientation. It never infers
that two different predicates are equivalent. The complete `Candidates` list
contains each advisor result in ranking order, so callers can expose the
reason for the choice in an explain plan.

## Ranking

Each alternative first goes through `AdviseTypedTableJoinArrangement`. The
selector then ranks the resulting plans by:

1. lower incremental `TotalBytes`;
2. lower transient bytes;
3. lower persistent bytes;
4. reuse before hydrate-then-reuse before create;
5. more shared references and newer checkpoints;
6. lower estimated state rows, then the deterministic predicate key.

The memory numbers are planning estimates. The caller must provide realistic
`EstimatedStateRows` values and still perform the normal acquire/hydrate/release
workflow after selection.

## Benchmark

Machine: AMD Ryzen 9 5950X, linux/amd64. Each result is the median of five
benchmark samples from `make benchmark-mu45-baseline` and
`make benchmark-mu45`.

| Operation | Median ns/op | B/op | allocs/op | Compared with exact advisor |
| --- | ---: | ---: | ---: | --- |
| Existing exact advisor, one predicate | 190.4 | 184 | 2 | baseline |
| Selector, one alternative | 261.6 | 328 | 3 | 1.37x time, 1.78x bytes, +1 alloc |
| Selector, three alternatives | 869.7 | 1,024 | 9 | 4.57x time, 5.56x bytes, +7 allocs |

This is planner overhead, not per-row join-update overhead. The selector is
useful when avoiding a larger or stale arrangement saves more memory and replay
work than this one-time decision costs. For a single known predicate, callers
should continue using the exact advisor directly.
