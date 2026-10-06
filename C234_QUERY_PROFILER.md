# C234 Query Profiler Allocation Accounting

The SQL query profiler already records bounded per-operator samples containing
stage CPU time, blocked time, rows, and bytes. C234 adds two allocation fields
to those samples:

- `AllocatedBytes`
- `AllocatedObjects`

They are caller-supplied stage counters, so an executor can record its own
operator allocation measurements without forcing runtime instrumentation on
all queries.

Query events also support opt-in whole-query allocation accounting:

```go
var event hatSql.QueryEvent
_, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, hatSql.SQLQueryOptions{
	Observer: hatSql.SQLQueryObserverFunc(func(observed hatSql.SQLQueryEvent) {
		event = observed
	}),
	ProfileAllocations: true,
})
if err != nil {
	return err
}
fmt.Println(event.AllocatedBytes, event.AllocatedObjects)
```

`ProfileAllocations` is off by default. When enabled, the event reports the
process-wide delta of Go `TotalAlloc` and `Mallocs` around the query. Other
goroutines may contribute to the delta, so it is a workload signal rather
than an exact per-query ownership measurement. The counters are emitted only
when an observer or slow-query recorder is active.

The allocation read is deliberately opt-in. On the benchmark fixture it costs
about `5.6x` query CPU and `+28 B/op`; the default path retained the same heap
and allocation counts. Enable it for diagnostics, sampled profiling, or slow
query investigations rather than permanently on a hot production path.

See [BENCHMARK.md#c234-query-profiler-allocation-accounting](BENCHMARK.md#c234-query-profiler-allocation-accounting)
for raw before/after runs.
