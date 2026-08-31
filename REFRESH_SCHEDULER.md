# Managed Refresh Scheduler

`hatSql.ManagedRefreshScheduler` runs named materialized-view and rollup
refreshes from an application-controlled loop. It does not start a goroutine,
and all new budget controls are off by default.

## Bounded Cycle

```go
scheduler, err := hatSql.NewManagedRefreshScheduler(hatSql.ManagedRefreshSchedulerOptions{
	MaxRunsPerCycle:  4,
	MaxCycleDuration: 200 * time.Millisecond,
})
if err != nil {
	return err
}

for {
	_, err := scheduler.RunDue(ctx)
	if err != nil {
		return err
	}
	// Wait according to the service's own worker-loop policy.
}
```

`MaxRunsPerCycle` limits the number of due tasks selected by one `RunDue`
call. Remaining due tasks are left for the next call in deterministic name
order. `MaxCycleDuration` stops starting new tasks after the cycle budget and
passes its remaining duration through each refresh callback context. Callbacks
must honor context cancellation to stop their own work promptly; the scheduler
does not interrupt a callback or publish partial results.

The controls compose. Use count-only limiting when stable work batching is
enough. Add a duration budget when callbacks perform cooperative I/O or CPU
work and must yield to foreground traffic.

## Defaults And Tradeoff

Both fields default to zero, preserving the existing unrestricted scheduler.
On an AMD Ryzen 9 5950X, one no-op due refresh had these median costs:

| Mode | Time | Heap | Allocations |
|---|---:|---:|---:|
| Default | 186 ns | 80 B | 1 |
| `MaxRunsPerCycle: 1` | 186 ns | 80 B | 1 |
| Count plus `MaxCycleDuration` | 568 ns | 352 B | 5 |

The timeout context has a deliberate cost. It is appropriate only for
background callbacks where cooperative yielding matters more than sub-microsecond
scheduler overhead.

## Guarantees

- Tasks remain non-overlapping with themselves.
- A task error retains the prior materialized-view snapshot and ends the
  current `RunDue` call, as before.
- Negative budgets are rejected at construction.
- Existing callers that provide only `Now` retain the prior scheduling and
  execution behavior.

## Verification

```sh
make test-sql-refresh-scheduler-budget
make benchmark-sql-refresh-scheduler-budget
make verify-sql-refresh-scheduler-budget
```
