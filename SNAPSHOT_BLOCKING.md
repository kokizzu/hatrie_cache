# Snapshot Blocking

`hatSql.SQLSourceFrontierTracker` now provides a cancellation-aware admission
barrier for readers that depend on a consistent source snapshot.

```go
tracker, err := hatSql.NewSQLSourceFrontierTracker([]hatSql.SQLSourceFrontierPartition{
    {Source: "orders", Partition: "eu"},
    {Source: "orders", Partition: "us"},
})
if err != nil {
    return err
}

if err := tracker.WaitReady(ctx, requiredFrontier); err != nil {
    return err
}
// Every configured partition has reached requiredFrontier here.
rows, err := readSnapshot()
```

`Observe` remains monotone and idempotent. `CommonFrontier`, `ReadyAt`, and
`Snapshot` continue to expose the current per-partition state for queryable
status and diagnostics. `WaitReady` wakes when any observed frontier advances
and returns `ctx.Err()` if the source never reaches the requested frontier.

The tracker does not create a notification channel until a caller waits. A
normal tracker that only observes or polls therefore keeps its previous
allocation behavior. Once waiting is enabled, each frontier advance signals
all waiters and creates the next wait generation; this is bounded to one small
channel per active generation and avoids polling. The channel is released when
the last waiter exits, so later ordinary observations return to the no-waiter
path.
