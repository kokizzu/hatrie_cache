# CH050 Plan Reproducibility Hash

`hatSql.SQLPlanReproducibilityHash` produces a bounded SHA-256 identity for a
logical SQL plan. It combines:

- the existing literal-independent `SQLQueryFingerprint`;
- a caller-provided schema fingerprint;
- a caller-provided settings fingerprint; and
- the structural `ExplainStep` plan.

The query parser still validates the SQL. Formatting and literal values do not
change the query component of the result, while schema, settings, or plan-shape
changes do.

## Usage

```go
hash, err := hatSql.SQLPlanReproducibilityHash(
	hatSql.SQLPlanReproducibilityInput{
		Query:               "SELECT id FROM users WHERE id = 7",
		SchemaFingerprint:   schemaFingerprint,
		SettingsFingerprint: settingsFingerprint,
		Steps:               explainSteps,
	},
)
if err != nil {
	return err
}
log.Printf("plan reproducibility hash=%s", hash)
```

Schema and settings fingerprints are required and capped at 4 KiB each. Plans
are capped at 4,096 steps and 1 MiB after deterministic JSON encoding. The
function never mutates the caller's plan.

Runtime-only fields are intentionally excluded: worker assignment, pruning
observations, actual row/byte counts, estimate errors, and elapsed time. Stable
plan fields such as operators, details, estimates, selected alternatives,
indexes, arrangements, projection, and lineage remain part of the digest.

This API is opt-in. Normal query execution and existing `EXPLAIN` responses do
not compute a hash unless a caller requests one, so there is no default query
latency or allocation cost.

## Measured Tradeoff

The benchmark uses a three-step plan and a representative SELECT query, with
five runs on the repository's AMD Ryzen 9 5950X host:

| Operation | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Existing `SQLQueryFingerprint` baseline | 11,520 | 8,448 | 41 |
| `SQLPlanReproducibilityHash` | 15,912 | 10,342 | 58 |

The complete plan identity costs about 1.38x the query-fingerprint-only path,
1.22x the bytes, and 1.41x the allocations. That is an intentional diagnostic
tradeoff for a stronger cross-node identity; keeping it opt-in avoids imposing
that cost on ordinary queries.

Tests and the benchmark are in `hat/hatSql/ch050_plan_reproducibility_hash_test.go`
and `hat/hatSql/ch050_plan_reproducibility_hash_benchmark_test.go`.
