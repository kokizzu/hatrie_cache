# Arrangement Hydration Status and Admission

Typed aggregate and join arrangements can be maintained incrementally while a
caller hydrates them from a source. Before this feature, callers could inspect
`Freshness()` and call `Hydrate(limit)`, but there was no first-class way to
wait for a source sequence before admitting a query.

The opt-in API adds progress snapshots and context-aware readiness waits:

- `TypedTableAggregateArrangement.HydrationStatus()` reports the arrangement
  checkpoint, current source sequence, and whether the arrangement is ready or
  stale relative to that source tail.
- `TypedTableAggregateArrangement.WaitReady(ctx, target)` waits until the
  aggregate has applied `target` or returns context cancellation, a hydration
  error, or `ErrTypedTableArrangementHydrationTargetAhead` when `target` is
  newer than the source sequence currently observed by the arrangement.
- `TypedTableJoinArrangement.HydrationStatus()` reports both left and right
  checkpoints and source sequences.
- `TypedTableJoinArrangement.WaitReady(ctx, leftTarget, rightTarget)` applies
  the same contract independently to the two join inputs.

## Usage

```go
status := arrangement.HydrationStatus()
target := status.SourceSequence

if err := arrangement.WaitReady(ctx, target); err != nil {
	return err
}

rows := arrangement.Rows()
```

For a join, pass the source sequences captured for each side:

```go
status := join.HydrationStatus()
if err := join.WaitReady(ctx, status.LeftSourceSequence, status.RightSourceSequence); err != nil {
	return err
}
```

`WaitReady` is caller-driven. It does not start a goroutine or reread the
source; another caller must run `Hydrate` or apply source changes. A waiter is
woken by those maintenance calls, by a hydration error, or by context
cancellation. Multiple waiters share one notification channel per arrangement
entry.

The default maintenance path remains unchanged. When there are no waiters,
successful `Apply` and `Hydrate` calls do not allocate a notification channel
or perform extra signaling work. The status/wait contract is therefore opt-in
control-plane behavior rather than a per-row query cost.

Hydration errors are retained until a later successful maintenance operation,
so a waiter cannot silently admit a query after a failed refresh. The API does
not change query-planner admission automatically; callers decide which target
frontier is required for a query and invoke the wait before reading the
arrangement.

## Verification

The focused tests cover aggregate and join progress, partial hydration,
blocking wake-up, cancellation, target-ahead rejection, and stale-to-ready
transitions. The package tests, race tests, and vet pass through the M-U36
Makefile targets.

See the paired before/after measurements in
[BENCHMARK.md](BENCHMARK.md#mu-036-arrangement-hydration-admission).
