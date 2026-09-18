# CH-009: TTL Rollup

CH-009 adds an explicit, importable summary for rows removed by row TTL. It
reuses `TypedTableAggregate` for exact grouped `COUNT`, optional `SUM`, `MIN`,
`MAX`, and `COUNT DISTINCT` state, but applies each expired row's `Before`
image with a positive delta. The detail row can therefore be deleted while a
coarser historical summary remains available.

## Defaults

The feature is off by default. `TypedTableTTLScheduler.Register` keeps its
existing behavior and never creates a rollup. Use
`RegisterWithRollup(name, table, rollup)` to opt in. No goroutine starts until
the caller explicitly calls `Start`; `RunOnce` remains available for a
deterministic maintenance pass.

## Example

```go
now := time.Now()
events, _ := hatSql.NewTypedTable(hatSql.TypedTableSchema{
    Name: "events",
    TTL: hatSql.TypedTableTTLOptions{
        Mode:     hatSql.TypedTableTTLProcessingTime,
        Lifetime: time.Hour,
    },
    Columns: []hatSql.TypedTableColumn{
        {Name: "region", Kind: hatSql.TypedTableString},
        {Name: "bytes", Kind: hatSql.TypedTableInt64},
    },
})
rollup, _ := hatSql.NewTypedTableTTLRollup(events,
    hatSql.TypedTableAggregateDefinition{
        GroupBy:  []string{"region"},
        SumField: "bytes",
    })
scheduler, _ := hatSql.NewTypedTableTTLScheduler(hatSql.TypedTableTTLSchedulerOptions{
    Now: func() time.Time { return now },
})
_ = scheduler.RegisterWithRollup("events", events, rollup)
_, _ = scheduler.RunOnce(context.Background())
summary := rollup.Rows()
```

For expired rows `region=apac, bytes=2` and `region=apac, bytes=3`, the
summary contains `{"region":"apac", "count":2, "sum":5}`. The rollup
checkpoint ignores a replayed expiry sequence. Expiry batches may have gaps
because non-expiry mutations are not part of the rollup input, but new expiry
sequences must remain increasing.

## Scope and recovery

This is an in-memory expired-row summary. The source `Before` images are not
copied into another detail table, and no automatic persistence or restore
replay is implied. Persist the rollup at the application boundary if it must
survive restart, and restore/replay from a known expiry sequence before
serving historical totals. Column TTL remains separate: creating a rollup for a
table with configured column TTL is rejected, so an expired column cannot be
accidentally retained in a row-expiry summary.

## Measurement

The paired benchmark uses 4,096 processing-time expired rows and 16 groups.
The rollup adds a small maintenance cost because it hashes/group-updates each
expired image. See [BENCHMARK.md](BENCHMARK.md#ch-009-ttl-rollup) for raw
samples and the exact reproduction target.
