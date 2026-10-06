# M-U35 Snapshot Blocking

`hatPipeline.SnapshotCutoverCoordinator` now exposes `Wait(ctx, id)` for
dependents that must not read until an explicitly prepared cross-source
snapshot is committed.

```go
status, err := coordinator.Wait(ctx, "orders-snapshot-42")
switch {
case err == nil:
	// Every prepared source acknowledged the requested frontier.
	_ = status.Timestamp
case errors.Is(err, hatPipeline.ErrSnapshotCutoverAborted):
	// The source set could not produce a consistent snapshot.
case errors.Is(err, context.Canceled):
	// The dependent read stopped waiting without changing the cutover.
}
```

The coordinator still owns no source I/O and starts no goroutines. Sources
call `Acknowledge`, an operator or coordinator calls `Commit` or `Abort`, and
`Wait` blocks on a per-cutover notification channel. Terminal status is copied
before it is returned, so callers cannot mutate coordinator state. Multiple
waiters share the same notification without one goroutine per waiter.

`Wait` returns `ErrSnapshotCutoverAborted` with the detached abort reason when
the cutover is aborted, `context.Canceled` or another context error when the
caller stops waiting, and `ErrSnapshotCutoverNotFound` after terminal state is
explicitly forgotten. Ordinary snapshot and cutover APIs remain unchanged.

Verification:

```text
make format-mu035-snapshot-blocking
make verify-mu035-snapshot-blocking
make benchmark-mu035-snapshot-blocking
```

The feature adds one `done` channel per retained cutover. It has no polling
loop, no default background worker, and no extra work on `Acknowledge` beyond
the existing state transition.
