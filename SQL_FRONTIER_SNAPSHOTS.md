# Frontier-Bound SQL Snapshots

`BeginSQLFrontierSnapshot` combines the indexed source-frontier barrier with an
explicit storage-provider contract. It waits until every configured partition
has observed a requested frontier, then asks the provider to open an immutable
view at that exact frontier.

```go
tracker, err := hatSql.NewSQLSourceFrontierTracker([]hatSql.SQLSourceFrontierPartition{
	{Source: "orders", Partition: "0"},
	{Source: "orders", Partition: "1"},
})
if err != nil {
	return err
}
barrier, err := hatSql.NewSQLSourceFrontierBarrier(tracker)
if err != nil {
	return err
}

// Source readers publish these observations as their committed progress.
if _, err := barrier.ObserveBatch([]hatSql.SQLSourceFrontier{
	{Source: "orders", Partition: "0", Frontier: 42},
	{Source: "orders", Partition: "1", Frontier: 42},
}); err != nil {
	return err
}

snapshot, release, err := hatSql.BeginSQLFrontierSnapshot(
	ctx,
	ordersResolver,
	barrier,
	42,
)
if err != nil {
	return err
}
if release != nil {
	defer release()
}
_ = snapshot
```

The resolver must implement both `SQLSourceResolver` and:

```go
type SQLFrontierSnapshotProvider interface {
	hatSql.SQLSourceResolver
	BeginSQLSnapshotAt(context.Context, uint64) (hatSql.SQLSourceResolver, func(), error)
}
```

Legacy resolvers are rejected with
`ErrSQLFrontierSnapshotProviderUnsupported`; the helper never silently falls
back to an unbounded snapshot. A canceled context or an unavailable frontier
prevents provider invocation. A nil snapshot resolver invokes the returned
release callback once and returns `ErrSQLSnapshotResolverNil`.

The provider owns the physical atomicity guarantee: its returned view must bind
all source reads to the supplied frontier. The barrier only proves that the
configured progress observations reached that frontier. It does not copy or
pause sources, elect a leader, replicate progress, or alter ordinary SQL
execution.

## Measurement

Command: `make benchmark-m032d-frontier-snapshot`.

Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X. The
parent benchmark was run from the pre-feature `8ae6ba5` commit; the final
control uses the unchanged existing snapshot-provider dispatch.

| Path | Raw ns/op | Median ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: |
| Existing provider dispatch, before | 4.190; 4.104; 4.037; 4.110; 4.095 | 4.104 | 0 | 0 |
| Existing provider dispatch, final control | 4.973; 4.759; 4.666; 4.624; 4.624 | 4.666 | 0 | 0 |
| Frontier-bound provider, final | 8.589; 8.485; 8.213; 8.637; 9.162 | 8.589 | 0 | 0 |

The opt-in exact-frontier path costs about `3.92 ns` over the final control and
has no measured allocation or retained memory cost. This small absolute cost is
accepted for the stronger consistency contract; the ordinary SQL path has no
barrier wait, frontier argument, or new allocation.
