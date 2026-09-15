# CH-U04 External `DISTINCT` Spill

This feature extends the existing exact disk-backed set operator to imported
`EXTERNAL('name')` SQL sources. It adopts the same high-cardinality safety idea
used by ClickHouse external processing: a source can stream rows while the
executor keeps only bounded distinct-key runs in memory.

## Behavior

`ExecuteSQLQueryRows` selects this path only when all of the following are true:

- the query is a direct scalar `EXTERNAL('name')` source;
- `SELECT DISTINCT` has no joins, grouping, aggregates, windows, CTEs, unions,
  or `ORDER BY`;
- `MaxSetBytes`, `SpillDirectory`, and `MaxSpillBytes` are configured; and
- the resolver implements `hatSql.ExternalStreamSourceResolver`.

The executor writes bounded key-sorted runs, retains the first source ordinal
for each distinct projected row, and merges ordinals before invoking the row
callback. This preserves exact equality, NULL handling, stable first-occurrence
order, and projected-row semantics without materializing the source, set, or
result slice in the SQL executor.

Example:

```go
err := hatSql.ExecuteSQLQueryRows(ctx, `
FROM EXTERNAL('events') AS event
SELECT DISTINCT event.id, event.payload`, tables, nil, hatSql.QueryOptions{
    MaxSetBytes:    8 << 20,
    SpillDirectory: "/var/lib/hatrie-cache/spill",
    MaxSpillBytes:  4 << 30,
}, visit)
```

`hatSql.ExternalTables` implements the streaming interface. Its immutable table
snapshot and cancellation behavior are documented in
[CHU02_EXTERNAL_ORDER_SPILL.md](CHU02_EXTERNAL_ORDER_SPILL.md).

## Compatibility And Cleanup

The existing `ExternalSourceResolver` contract is unchanged. The materialized
`ExecuteSQLQuery` API continues to use `ResolveSQLExternalSource`; a resolver
without the optional stream method does not silently materialize an unbounded
`ExecuteSQLQueryRows` DISTINCT query. Spill files are removed on success,
callback failure, cancellation, and quota failure.

This is opt-in because disk encoding, filesystem I/O, and merge passes are
slower than an in-memory set for small sources. Operators should use it when
the source cardinality or memory limit makes retaining the complete distinct
set unsafe. `SpillDirectory` should have restrictive permissions and a quota.

## Verification And Measurement

```sh
make format-chu04-c243
make test-chu04-c243
make test-chu04-package-c243
make race-chu04-c243
make vet-chu04-c243
make benchmark-chu04-c243
make memory-chu04-c243
```

The benchmark uses the same generated 4,096-row source for the prior
materialized path and the streaming path, with `MaxSetBytes=16 KiB` and a
64 MiB spill budget. Results, including cumulative allocation and process RSS,
are recorded in [BENCHMARK.md](BENCHMARK.md).
