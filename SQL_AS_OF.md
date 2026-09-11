# SQL AS OF historical reads

`hatSql` supports opt-in historical SQL reads through
`SQLQueryOptions.AsOfFrontier`. The option binds one query execution to an
immutable resolver snapshot at an exact logical frontier.

The default remains the live execution path. A nil `AsOfFrontier` does not
change existing behavior. Use a pointer when requesting a historical read so
frontier `0` remains distinguishable from the default:

```go
func runHistorical(ctx context.Context, resolver hatSql.SQLFrontierSnapshotProvider) (hatSql.SQLQueryResult, error) {
	frontier := uint64(42)
	return hatSql.ExecuteSQLQueryParameters(
		ctx,
		"SELECT id, value FROM CACHE('events') WHERE id = $1",
		resolver,
		[]interface{}{int64(7)},
		hatSql.SQLQueryOptions{AsOfFrontier: &frontier},
	)
}
```

The application resolver must implement `SQLFrontierSnapshotProvider`:

```go
type SQLFrontierSnapshotProvider interface {
	SQLSourceResolver
	BeginSQLSnapshotAt(ctx context.Context, frontier uint64) (
		resolver SQLSourceResolver,
		release func(),
		err error,
	)
}
```

`BeginSQLSnapshotAt` is responsible for returning a resolver that is stable at
the requested frontier. The returned release function is called after query
execution, including error paths. Providers should reject unavailable or
unretained frontiers instead of silently returning a newer view.

## Covered APIs

The frontier is applied to all SQL execution forms that accept
`SQLQueryOptions`:

- `ExecuteSQLQueryParameters` and its non-prefixed query facade
- `ExecuteSQLQueryRows` for streamed row callbacks
- `ExecuteSQLQueryPage` for offset cursors
- `ExecuteSQLQueryKeysetPage` for ordered keyset cursors

Historical reads bypass the live SQL result cache. This prevents a result
computed at the current frontier from being returned for an older frontier.
Offset and keyset cursor fingerprints include the requested frontier, while
the nil/default fingerprint remains unchanged for existing cursors.

An unsupported resolver returns `hatSql.ErrSQLAsOfUnsupported`. Provider
errors and canceled contexts are returned unchanged. A provider returning a
nil snapshot resolver is rejected rather than falling back to live data.

This is an API-level historical-read contract; it does not add `AS OF` grammar
to SQL text. The provider owns frontier retention, source consistency, and
recovery policy.

## Measurement

The focused benchmark compares the same pointer-backed resolver shape with and
without `AsOfFrontier`:

```text
make benchmark-mz008-asof
```

Five-sample paired medians from the implementation run:

| Mode | Median ns/op | B/op | allocs/op | Difference |
| --- | ---: | ---: | ---: | --- |
| Live/default | 2,768 | 3,488 | 15 | baseline |
| Historical frontier | 2,770 | 3,488 | 15 | 0 B/op, 0 allocs/op; about 0.07% slower in this run |

The result shows no measurable allocation or memory cost for this small query.
The historical provider's own snapshot creation and retention costs are
application-specific and are not hidden by this benchmark.
