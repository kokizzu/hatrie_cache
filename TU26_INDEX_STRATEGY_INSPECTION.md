# T-U26 Index Strategy Hints and Inspection

T-U26 adds opt-in physical index strategy selection and deterministic strategy
inspection to `hatSql`. It is useful when a caller has multiple indexes for a
field and needs to force one implementation or explain why another candidate
was selected.

## API

`SQLIndexHint.Kind` names a physical strategy such as `HASH` or `ORDERED`.
The field is optional for compatibility with existing hints, but when set the
hint must use `SQLIndexHintForce` or `SQLIndexHintForbid`.

```go
options.IndexHint = hatSql.SQLIndexHint{
	Source: "p",
	Field:  "id",
	Kind:   "HASH",
	Mode:   hatSql.SQLIndexHintForce,
}
```

Resolvers that support a named strategy implement
`StrategyIndexedSourceResolver` for equality predicates and
`StrategyRangeIndexedSourceResolver` for ordered predicates. A kind-specific
FORCE hint fails with `ErrSQLIndexStrategyHintUnsupported` when the resolver
does not expose the strategy-aware interface or the requested index is not
available. Empty `Kind` keeps the existing resolver behavior.

Kind-specific FORBID hints deliberately return the same unsupported error from
query execution instead of silently falling back to a different index. Use
`ExplainSQLIndexStrategy` to inspect candidates and apply a caller-owned
forbid policy before executing a query.

```go
decision, err := hatSql.ExplainSQLIndexStrategy(
	"people",
	"id",
	hatSql.SQLIndexHint{Field: "id"},
	[]hatSql.SQLIndexStrategyCandidate{
		{
			SQLIndexDefinition: hatSql.SQLIndexDefinition{Key: "by_id", Field: "id", Kind: "HASH"},
			Priority:            10,
			EstimatedRows:       1,
			EstimatedCost:       1,
			Available:            true,
		},
	},
)
if err != nil {
		return err
}
if decision.HasSelection {
	// decision.Selected.Kind is the deterministic choice.
}
for _, candidate := range decision.Candidates {
	// candidate.Eligible and candidate.Reason explain the outcome.
}
```

Candidates are ranked by field match, availability, lower priority, lower
estimated cost, lower estimated rows, and stable key/kind tie-breakers. The
report does not build an index, access data, or change planner state. It only
describes the caller-supplied candidate set.

## Compatibility and limits

- Existing empty-kind FORCE and FORBID hints retain their prior behavior.
- Strategy-aware resolver methods must return candidate rows; SQL still
  rechecks the original predicate before publishing results.
- Strategy names are compared case-insensitively and passed to resolvers in
  uppercase.
- The inspection helper is bounded by the candidate slice supplied by the
  caller and is deterministic, but it is not a cost model or an automatic
  index advisor.
- The API adds no background worker, server, persistence format, or default
  index. Callers own candidate discovery, authentication, and policy.

## Measurement

Five `-benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | Median CPU | Memory | Interpretation |
| --- | ---: | ---: | --- |
| Existing-style candidate selection control | 8.46 ns/op | 0 B/op, 0 allocs/op | Hot selection baseline |
| `ExplainSQLIndexStrategy` with four candidates | 518.3 ns/op | 600 B/op, 4 allocs/op | Explicit inspection only |

The inspection path is approximately 61.3x slower than the tiny control loop,
so it must remain off the query hot path. Ordinary query execution does not
call it. Reproduce with `make benchmark-tu26`.
