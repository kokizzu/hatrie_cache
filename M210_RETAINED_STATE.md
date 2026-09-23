# M210: Retained Logical State

`hatSql.SQLRetainedState` is a bounded in-memory implementation of exact
historical `AS OF` reads. It fills the gap between the SQL executor's existing
`SQLFrontierSnapshotProvider` contract and storage adapters that need a small,
self-contained retained state for tests, embedded services, projections, or
cache-backed query views.

## Usage

```go
state, err := hatSql.NewSQLRetainedState(hatSql.SQLRetainedStateOptions{
	MaxFrontiers: 64,
})
if err != nil {
	return err
}
if err := state.Publish(100, []hatSql.SQLRetainedStateSource{
	{
		Name: "CACHE",
		Key:  "users",
		Rows: []hatSql.Row{{"id": int64(1), "name": "Ada"}},
	},
}); err != nil {
	return err
}

frontier := uint64(100)
result, err := hatSql.ExecuteSQLQueryContext(ctx,
	"FROM CACHE('users') SELECT id, name",
	state,
	hatSql.SQLQueryOptions{AsOfFrontier: &frontier},
)
```

`Publish` atomically replaces only the named sources supplied in the batch;
omitted sources remain unchanged. Frontiers must be strictly increasing and
positive. A publish validation failure leaves the current view and retained
history untouched. `SQLRetainedState` also implements
`HistoricalSourceResolver`, so it can be used as the resolver for a query
subscription with `QuerySubscriptionDefinition.AsOf`.

Frontier zero is the immutable empty initial view. Nonzero reads require the
exact frontier to still be retained; a request for a missing or evicted
frontier returns `ErrSQLRetainedStateFrontierUnavailable`. The state does not
silently choose an older frontier because that would weaken `AS OF` snapshot
semantics.

## Bounds and memory

Zero options select bounded defaults:

| Option | Default |
| --- | ---: |
| `MaxFrontiers` | 64 |
| `MaxSources` | 1,024 |
| `MaxRows` | 1,000,000 current rows |

Each version copies the source index, but unchanged source row slices are
shared. Changed input rows are copied at `Publish`, and public
`ResolveSQLSource` calls return independent row copies. The SQL executor can
use the internal immutable borrowed-source path without copying again; callers
must use `ResolveSQLSource` when they need an independently mutable result.

This is an in-memory retention primitive, not durable storage or replication.
Persist the source updates and rebuild the state after restart. It retains
complete source replacements rather than interpreting row-level deltas, which
keeps snapshot construction deterministic and makes source multiplicity the
caller responsibility.

## Measurement

On Linux/amd64 with an AMD Ryzen 9 5950X, five benchmark samples measured:

| Workload | Median | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Retained-state `AS OF` query | 10,935 ns | 7,320 | 30 |
| Trivial direct provider `AS OF` query | 8,360 ns | 5,912 | 23 |
| Four-row retained-state publish | 1,979 ns | 2,016 | 12 |

The retained query is about 1.31x the trivial provider baseline, with 23.8%
more allocated bytes and seven additional allocations. That overhead is the
cost of exact retained-version lookup, immutable state isolation, and the
bounded copy-on-write source index; it is not an optimization claim. The
advantage is that callers receive real historical views without implementing a
custom snapshot provider. Raw samples are in
[BENCHMARK.md#m210-retained-logical-state](BENCHMARK.md#m210-retained-logical-state).

Focused verification:

```text
make m210-test
make m210-race
make m210-vet
make m210-benchmark
```
