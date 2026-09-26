# Optional SQL Compute Pool

MZ-018 is partially adopted as an opt-in compute admission boundary for
managed SQL queries. It is inspired by Materialize's separation of compute
workers from maintained state and by ClickHouse's explicit query worker
controls.

`SQLQueryManager` can execute managed queries on a bounded
`hatPipeline.WorkStealingPool`. The resolver and snapshot are still supplied
by the caller, so this is not a distributed compute/storage split and it does
not move durable writes into a separate process.

## Resolver-Only Compute Adapters

M090a adds an explicit resolver-only registration path for a stateless SQL
compute process. `hatStorage.SQLResolverAdapter` implements the same SQL
namespace contract without opening a local `hatStorage.Engine`:

```go
registry, err := hatStorage.NewSQLAdapterRegistry(nil, hatStorage.SQLResolverAdapter{
	NamespaceName: "eu-west",
	Resolver: remoteSnapshotResolver,
})
```

`Execute` uses the normal parser and executor, while the supplied resolver
owns remote transport, snapshot consistency, authentication, retries, and
data locality. `Inspect` returns
`hatStorage.ErrSQLAdapterStorageUnavailable` because there is no local engine
to inspect. The existing `SQLNamespaceAdapter` still rejects a missing engine,
so local storage remains the default and no accidental remote mode is
introduced.

This is a registration boundary, not a distributed database protocol. It does
not replicate data, coordinate frontiers, or hide network failures. Those
responsibilities stay with the resolver or the service that supplies it.

## Context-Aware Materialized Sources

M090b adds an optional `hatSql.ContextSourceResolver` contract for remote
storage adapters that materialize a source as a row slice. Its
`ResolveSQLSourceContext` method receives the query context, so a canceled or
timed-out compute request can stop a remote fetch before the whole source is
returned:

```go
type RemoteResolver struct{}

func (RemoteResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	return fetchSnapshot(context.Background(), name, key)
}

func (RemoteResolver) ResolveSQLSourceContext(ctx context.Context, name, key string) ([]hatSql.Row, error) {
	return fetchSnapshot(ctx, name, key)
}
```

The SQL executor prefers the context-aware method for materialized cache/keys
sources, including the native dataflow and `EXPLAIN` snapshot paths. Ordinary
`SourceResolver` implementations remain source-compatible. `SQLSession` and
`CatalogResolver` forward the optional contract; partition, borrowed, and
streaming resolver extensions keep their existing precedence.

This does not make a remote transport interruptible by itself: the resolver
must pass `ctx` to its database, HTTP, or RPC client. It also does not change
the row payload format or reduce bandwidth for successful reads. Its benefit
is cancellation propagation and avoiding work after the caller has gone away.

## Configuration

The default is unchanged:

```go
manager := hatSql.NewSQLQueryManager(256)
```

Enable an independent query compute pool only when the service wants a fixed
query CPU budget and bounded waiting queue:

```go
manager := hatSql.NewSQLQueryManagerWithOptions(hatSql.SQLQueryManagerOptions{
	HistoryCapacity:      256,
	ComputeWorkers:       4,
	ComputeQueueCapacity: 64,
})
defer manager.Close()
```

`ComputeWorkers: 0` is the default and keeps execution in the caller's
goroutine. When workers are positive, a zero `ComputeQueueCapacity` selects
`DefaultSQLQueryManagerComputeQueueCapacity` (`64`). The queue counts waiting
queries; up to `ComputeWorkers` queries may also be running.

Use `ValidateSQLQueryManagerOptions` during service configuration loading. The
current hard limits are `MaxSQLQueryManagerComputeWorkers` (`256`) and
`MaxSQLQueryManagerComputeQueueCapacity` (`100000`). Invalid settings are
returned by later `Execute` calls because the existing constructor API returns
only a manager for compatibility.

`Close` rejects new managed queries and drains admitted work. It is idempotent.
It does not cancel a running query, so callers should cancel their request
contexts when shutdown must finish promptly.

## Interaction With Query Workers

`SQLQueryManagerOptions.ComputeWorkers` limits concurrent managed queries.
`SQLQueryOptions.Workers` remains the per-query operator parallelism setting.
Using both can multiply goroutine and CPU demand, so keep the product within
the host's CPU and memory budget.

The compute pool retains no SQL text, source names, parameters, or result rows
itself. A bounded queue does retain the task closure and its caller-owned
references until the task runs or admission is canceled.

## Benchmark Tradeoff

The benchmark uses a deterministic one-row `CACHE` query and a static resolver,
with `go test -cpu 32 -benchmem -count=5` on `linux/amd64` and an AMD Ryzen 9
5950X. The pool is deliberately measured against the existing manager path.

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative to legacy concurrent |
| --- | ---: | ---: | ---: | ---: |
| Legacy manager, concurrent callers | 3,597 | 3,506 | 23 | `1.00x` |
| Four-worker compute pool, concurrent callers | 3,982 | 4,419 | 30 | `1.11x` time, `1.26x` memory |
| One-worker compute pool, sequential caller | 5,751 | 4,400 | 30 | use only for isolation |

The isolated pool is therefore useful for admission control and predictable
resource ownership, not for accelerating tiny queries. It remains default-off
because the measured queue, closure, and synchronization cost is not justified
for ordinary low-contention execution.

See the raw samples and baseline comparison in
[BENCHMARK.md#mz-018-optional-sql-compute-pool](BENCHMARK.md#mz-018-optional-sql-compute-pool).

The resolver-only adapter measurement is recorded in
[BENCHMARK.md#m090a-resolver-only-sql-compute-adapter](BENCHMARK.md#m090a-resolver-only-sql-compute-adapter).
The context-aware materialized resolver measurement is recorded in
[BENCHMARK.md#m090b-context-aware-materialized-source-resolver](BENCHMARK.md#m090b-context-aware-materialized-source-resolver).

## Projected Materialized Sources

M090c adds the optional `hatSql.ProjectedSourceResolver` contract for a
materialized source that can fetch only selected fields. The SQL executor
automatically uses it for conservative single-source `SELECT` and `WHERE`
shapes, and the automatic native scalar dataflow path uses the same hook.
`ContextProjectedSourceResolver` is preferred when the adapter also needs
request cancellation.

The field list contains parsed field identifiers only. Joins, aggregates,
ordering, windows, CTEs, unions, partition-aware resolvers, cached source
materializations, and unsupported expressions retain the ordinary full-row
path. Returning `available=false` is the compatibility escape hatch for
adapters that cannot project a particular source. Existing resolvers do not
need to change. See [M090C_PROJECTED_SOURCE.md](M090C_PROJECTED_SOURCE.md) for
the contract, example, and measured transport tradeoff.
