# Deterministic Row Sampling

`SampleSQLRows` provides deterministic, key-based sampling inspired by
ClickHouse `SAMPLE` semantics.

```go
sample, err := hatSql.SampleSQLRows(rows,
	func(row hatSql.SQLRow) string { return row["account_id"].(string) },
	0.25,
	2026,
)
```

The same key, fraction, and seed always have the same selection, regardless of
input order or which partition supplies the row. The returned slice preserves
the input order. Fraction `0` returns nil and fraction `1` returns all rows;
other values must be finite and between zero and one. The key callback must be
deterministic. Returned row maps are the original row values, so callers must
treat sampled rows as read-only unless they intentionally want to mutate the
source view.

The implementation uses `xxhash` followed by a fixed integer mixing step. It
is intended for reproducible workload reduction and distributed sampling, not
for security, secrecy, or adversarially fair randomization.

On the reference host, sampling 1,024 rows at fraction `0.25` with precomputed
keys took about `15.97-16.30 us`, used `2,304 B/op`, and performed one
allocation for the selected slice. The cost is paid only by callers that opt
into sampling; no existing SQL or partition path changes its default behavior.
