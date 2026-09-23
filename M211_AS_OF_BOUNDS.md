# M211: Explicit AS OF Bounds

M211 adds optional interval validation for historical SQL reads. The interval
is `[AsOfSince, AsOfUpper)`: `since` is inclusive and `upper` is exclusive.
The feature is disabled when both bounds are nil, preserving existing query
behavior.

## Usage

```go
frontier := uint64(100)
since := uint64(90)
upper := uint64(120)
result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver,
	hatSql.SQLQueryOptions{
		AsOfFrontier: &frontier,
		AsOfSince:    &since,
		AsOfUpper:    &upper,
	})
```

The requested frontier must satisfy:

```text
since <= AsOfFrontier < upper
```

`AsOfSince` or `AsOfUpper` without `AsOfFrontier` is rejected with
`ErrSQLAsOfBoundsRequireFrontier`. A reversed or empty interval is rejected
with `ErrSQLAsOfBoundsInvalid`. A frontier below `since` returns
`ErrSQLAsOfBeforeSince`; a frontier at or above `upper` returns
`ErrSQLAsOfAtOrAfterUpper`.

Validation happens after a signed `SnapshotToken` is authenticated and
normalized to `AsOfFrontier`, but before a source snapshot/provider is opened.
The same check is used by materialized reads, row streams, offset pages, and
keyset pages. Rejected requests therefore do not invoke the source provider or
row callback.

The bounds are query-level admission constraints. They do not retain history,
change source data, or make an unsupported resolver historical; a resolver
still needs the existing `SQLFrontierSnapshotProvider` contract for `AS OF`
execution.

## Cost

The normalization check adds no allocations. On Linux/amd64 with an AMD Ryzen
9 5950X, the focused primitive benchmark measured a 3.41 ns median default
normalization and a 5.54 ns median bounded normalization. The extra check is
about 2.14 ns in this microbenchmark. Raw samples are in
[BENCHMARK.md#m211-explicit-as-of-bounds](BENCHMARK.md#m211-explicit-as-of-bounds).

Focused verification:

```text
make m211-test
make m211-race
make m211-vet
make m211-benchmark
```
