# Differential Operators

`hatSql` exposes a small reusable operator layer for `(row, time, diff)`
streams. It is useful to build incremental views and custom dataflow edges
without losing negative updates or duplicate multiplicity.

## Supported Operations

- `FilterDifferentialRows` applies selection and preserves every non-zero
  signed weight.
- `MapDifferentialRows` applies a one-to-one projection and consolidates rows
  with the same output key and timestamp.
- `FlatMapDifferentialRows` supports one-to-many transforms such as array
  expansion. Each emitted row inherits the input timestamp and weight.
- `UnionDifferentialRows` combines `UNION ALL`-style batches and consolidates
  equal identities.
- `JoinDifferentialRows` computes an inner join. A matching pair contributes
  `left.Diff * right.Diff`, including negative weights and duplicate
  multiplicity.
- `GroupSumInt64DifferentialRows` maintains signed `SUM` transitions for
  callback-defined groups, including weighted updates and exact overflow
  checks.

All functions return no partial output when a callback fails. Input rows and
row maps are not mutated. Callback row maps are private clones and must be
treated as read-only. Output identities must be non-empty; equal output
identity and timestamp means the rows represent one multiset element and the
weights are summed with overflow checking.

```go
updates, err := hatSql.FilterDifferentialRows(rows, func(row hatSql.Row) (bool, error) {
	return row["region"] == "sg", nil
})
if err != nil {
	return err
}

joined, err := hatSql.JoinDifferentialRows(left, right,
	func(left, right hatSql.Row) (bool, error) {
		return left["id"] == right["id"], nil
	},
	func(left, right hatSql.Row) (string, hatSql.Row, error) {
		return "order:" + left["id"].(string), hatSql.Row{
			"order_id": left["id"],
			"customer": right["name"],
		}, nil
	},
)
```

For a join, the output timestamp is `max(left.Time, right.Time)`. A positive
row joined with a negative row emits a negative result; two negative rows emit
a positive result. Weight multiplication and consolidation reject `int64`
overflow instead of wrapping.

## Scope And Cost

This is a reusable Go API, not a new SQL parser syntax or an automatic
replacement for the existing query executor. The generic join is a nested-loop
fallback. Use typed-table equality/range arrangements or the existing indexed
SQL paths for large relations. The generic callbacks are intentionally
allocation-safe at the ownership boundary, but cloning arbitrary `Row` maps
still costs memory.

The public-input safety contract is important for asynchronous or retryable
pipelines: callers can reuse or mutate their source rows after the call. The
implementation avoids a second clone while consolidating output that it
already owns.

## Verification

Focused correctness, race, package, and vet checks cover signed weights,
duplicate consolidation, one-to-many expansion, latest timestamp selection,
callback failures, input ownership, and multiplication overflow. The
benchmark is available through `make benchmark-differential-operators-clean`.
