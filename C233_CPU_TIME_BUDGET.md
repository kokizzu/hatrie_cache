# C233 CPU-Time Budgets

`hatSql.SQLQueryOptions.MaxCPUTime` adds an opt-in cooperative CPU-time budget
to one SQL query. The budget is checked at the execution-control checkpoints
already used for context cancellation, row limits, and execution-step limits.

```go
clock := hatSql.NewSQLCPUTimeSource(readQueryCPUTime)
result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil, hatSql.SQLQueryOptions{
	MaxCPUTime:     50 * time.Millisecond,
	CPUTimeSource: clock,
})
```

`readQueryCPUTime` is supplied by the embedding application because Go does
not expose one portable CPU-clock definition for a query that may run across
multiple worker goroutines. The reader must be monotonic and safe for
concurrent calls when query workers are enabled. It should account for the
same workers that execute the query, not only the caller goroutine.

The default is unchanged: `MaxCPUTime == 0` does not read `CPUTimeSource` and
does not require one. A positive budget without a source is rejected with
`ErrSQLCPUTimeSourceRequired`; negative budgets are rejected as invalid.
Cancellation remains cooperative, so a custom function or resolver that does
not return to the executor cannot be forcibly interrupted by this option.
Use `SQLQueryOptions.Timeout` when a wall-clock deadline is required as well.

## Tradeoffs

| Mode | CPU-check latency | Allocations | Behavior |
| --- | ---: | ---: | --- |
| Origin, budget disabled | 4.96-5.24 ns/op | 0 B/op, 0 allocs/op | Existing execution check |
| C233, budget disabled | 4.76-5.59 ns/op | 0 B/op, 0 allocs/op | Clock is not read |
| C233, budget enabled | 6.54-7.25 ns/op | 0 B/op, 0 allocs/op | Reads the supplied clock and cancels at a checkpoint |

The enabled path adds roughly 1.8 ns at the median in this isolated
checkpoint benchmark, about 1.35x the origin median, with no allocation. The
disabled median was 4.88 ns/op versus 4.99 ns/op for origin in this run; the
option is therefore kept opt-in.
