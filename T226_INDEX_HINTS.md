# T226 Explicit Index Hints and Planner Diagnostics

This round-2 item records the ClickHouse-style diagnostic control of physical
index selection and the Materialize-style explainable planner decision. The
runtime implementation is in `hat/hatSql/index_hint.go`; this commit
re-verifies and documents the existing API without changing the default query
path.

## Behavior

- `SQLIndexHint` supports opt-in `FORCE` and `FORBID` controls for a source and
  field.
- `Kind` can require or forbid a named strategy such as `HASH` or `ORDERED`.
- Strategy-aware resolvers receive normalized uppercase strategy names.
- A forced unavailable strategy fails explicitly with
  `ErrSQLIndexStrategyHintUnsupported`; it never silently falls back to a
  scan.
- `ExplainSQLIndexStrategy` ranks caller-supplied candidates deterministically
  and reports selection, eligibility, and rejection reasons.
- Inspection does not build indexes, read rows, mutate planner state, or add a
  background worker. Empty-kind hints preserve the legacy behavior.

Example:

```go
options.IndexHint = hatSql.SQLIndexHint{
	Source: "p",
	Field:  "id",
	Kind:   "HASH",
	Mode:   hatSql.SQLIndexHintForce,
}
```

For the inspection API and candidate-ranking contract, see
[TU26_INDEX_STRATEGY_INSPECTION.md](TU26_INDEX_STRATEGY_INSPECTION.md).

## Verification First

The existing focused tests were run before this documentation update:

```text
make test-sql-index-hints
make test-sql-index-advisor
make test-sql-projection-advisor
```

The T226 workflow then passed formatting, package tests, race detection, and
vet through `make verify-tu26`.

## Measurement

Five `-benchmem` samples from `make benchmark-tu26` on Linux/amd64 with an
AMD Ryzen 9 5950X produced these medians:

| Workload | Median CPU | Memory | Interpretation |
| --- | ---: | ---: | --- |
| Existing-style candidate selection control | 8.023 ns/op | 0 B/op, 0 allocs/op | Hot selection baseline |
| `ExplainSQLIndexStrategy` with four candidates | 486.4 ns/op | 600 B/op, 4 allocs/op | Explicit inspection only |

Inspection is about 60.6x slower than the tiny control loop and allocates 600
bytes per call. That is a diagnostic cost, not a query-path regression: the
API is opt-in and ordinary execution does not call it. The correct usage is to
run it when selecting or debugging a strategy, then execute with an explicit
hint only when the caller accepts that policy.
