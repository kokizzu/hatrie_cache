# CH-U02 External `ORDER BY` Spill

This feature adopts the external-sort part of ClickHouse's large-query
execution model for imported `EXTERNAL('name')` SQL sources. It lets a source
stream rows into the existing bounded external-sort operator instead of first
materializing the complete external table in the SQL executor.

## API

`hatSql.ExternalStreamSourceResolver` is an optional extension of the normal
`SQLSourceResolver` contract:

```go
type ExternalStreamSourceResolver interface {
    StreamSQLExternalSource(ctx context.Context, name string, visit func(Row) error) error
}
```

`hatSql.ExternalTables` implements the interface. It captures the immutable
table snapshot under its read lock, releases the lock before callbacks run, and
checks cancellation before every row. A concurrent `Register` therefore does
not block behind a query, and the running query continues to read its original
snapshot.

## When It Runs

The bounded path is selected by `ExecuteSQLQueryRows` only for a direct
`EXTERNAL('name')` source with:

- an unbounded scalar `ORDER BY`;
- positive `MaxSortBytes` and `MaxSpillBytes`;
- a configured `SpillDirectory`;
- no joins, grouping, aggregates, windows, CTEs, unions, or `DISTINCT`.

Finite `LIMIT` queries continue to use the cheaper bounded Top-N path where
available. The existing `CACHE` and `VALUES` spill paths are unchanged.

Example:

```go
tables := hatSql.NewExternalTables()
if err := tables.ImportJSON("events", payload); err != nil {
    return err
}
err := hatSql.ExecuteSQLQueryRows(ctx, `
FROM EXTERNAL('events') AS event
SELECT event.id, event.payload
ORDER BY event.id DESC`, tables, nil, hatSql.QueryOptions{
    MaxSortBytes:   8 << 20,
    SpillDirectory: "/var/lib/hatrie-cache/spill",
    MaxSpillBytes:  4 << 30,
}, visit)
```

The row callback receives output in the requested order. Stable ties, NULL
ordering, cancellation, result limits, spill quotas, and cleanup use the same
external-sort implementation as direct `CACHE` and `VALUES` sources. Temporary
run files are removed on success, callback failure, cancellation, and quota
failure.

## Compatibility And Operations

The new interface is optional and does not change `ExternalSourceResolver`.
Resolvers that only implement `ResolveSQLExternalSource` continue to support
the materialized `ExecuteSQLQuery` API. A row-streaming query that requires
external spill must provide the optional streaming method; it does not silently
materialize an unbounded source and defeat the configured memory limit.

`SpillDirectory` is application configuration. The process must have exclusive
access to a suitable directory with enough free space, and operators should
apply filesystem quotas and restrictive permissions. The SQL parser passes the
logical external table name to the resolver; it does not open a path or make a
network request.

## Verification And Measurement

```sh
make format-chu02-c242
make test-chu02-c242
make benchmark-chu02-c242
```

The benchmark compares the previous materialized external `ORDER BY` path with
the streaming spill path on the same 4,096-row fixture. `ns/op` measures CPU
and wall-clock time, while `B/op` and `allocs/op` expose Go heap pressure. The
streaming path is expected to trade additional spill I/O for bounded in-memory
sort state; the result table in [BENCHMARK.md](BENCHMARK.md) records that tradeoff
instead of presenting it as a universal speedup.
