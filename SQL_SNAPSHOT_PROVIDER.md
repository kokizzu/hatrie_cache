# SQL Snapshot Provider

This is the partial M032 adoption from the Materialize-inspired consistency
work: an SQL resolver may pin all source reads for one query to one immutable
view. It is useful when a logical source spans independent partitions or
regions and the storage layer already knows how to create a common read
frontier.

## API

Implement `hatSql.SQLSnapshotProvider` alongside the normal
`hatSql.SQLSourceResolver`:

```go
type SQLSnapshotProvider interface {
    BeginSQLSnapshot(context.Context) (hatSql.SQLSourceResolver, func(), error)
}
```

`BeginSQLSnapshot` must atomically create a read view and return it as the
resolver. The view must keep every source row and index result immutable for the
query lifetime. The release function is called once after materialized or
streamed execution, including query errors. A nil view is rejected with
`hatSql.ErrSQLSnapshotResolverNil`.

The executor uses the returned resolver for all source reads, including joins,
CTEs, nested queries, result pages, and streamed rows. A resolver that does
not implement `SQLSnapshotProvider` keeps the existing path with no snapshot
allocation or release callback.

## Example

```go
type database struct{}

func (db *database) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
    return db.readLive(name, key)
}

func (db *database) BeginSQLSnapshot(ctx context.Context) (hatSql.SQLSourceResolver, func(), error) {
    view, release, err := db.beginReadView(ctx)
    if err != nil {
        return nil, nil, err
    }
    return view, release, nil
}
```

The storage implementation owns the actual frontier protocol. It may use an
MVCC snapshot, a transaction, or a source-specific consistent read handle.
The SQL package does not guess a global timestamp or copy all rows, so it
cannot make arbitrary external sources consistent automatically. That remains
the intentionally open part of M032.

## Measurement

Workload: one prepared-shaped query over one row,
`FROM CACHE('items') WHERE state = 'ready' SELECT id`, Go benchmark with
`-benchmem -count=5` on an AMD Ryzen 9 5950X.

| Path | Sample ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: |
| Before provider, 5 samples | 4,759, 4,751, 4,561, 4,507, 4,329 | 5,120 | 33 |
| After, legacy resolver | 4,768, 4,713, 4,725, 4,648, 4,590 | 5,120 | 33 |
| After, snapshot provider | 4,639, 4,508, 4,509, 4,357, 4,409 | 5,120 | 33 |

The paired run shows no additional bytes or allocations. CPU samples overlap;
the small median difference is benchmark noise, not a claimed speedup. The
benefit is correctness for providers that can pin a common frontier, with no
cost on the legacy path.

## Verification

```text
make test-sql-snapshot-provider
make review-sql-snapshot-provider
make benchmark-sql-snapshot-provider
make test
```

The full test target covers normal tests, race tests, and coverage checks.

## Exact Frontier Extension

Providers that can bind an immutable view to a caller-selected source frontier
may implement `SQLFrontierSnapshotProvider` and use
`BeginSQLFrontierSnapshot`. The helper waits on
`SQLSourceFrontierBarrier`, passes the exact frontier to
`BeginSQLSnapshotAt`, and rejects legacy providers rather than silently
weakening consistency. See [SQL_FRONTIER_SNAPSHOTS.md](SQL_FRONTIER_SNAPSHOTS.md)
for the contract, example, and measured opt-in overhead.
