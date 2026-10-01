# Snapshot Readiness

`hatSql.SQLMultiSourceSnapshotCoordinator` already publishes a complete
multi-source snapshot atomically. The `WaitReady(ctx)` method adds an opt-in
admission gate for dependents that must not query before that first complete
view exists.

```go
view, err := coordinator.WaitReady(ctx)
if err != nil {
	return err
}
rows, err := view.ResolveSQLSource("CDC", "customers")
```

The method returns immediately after a successful capture or checkpoint
restore. A failed or canceled capture does not open the gate, and a caller's
context controls how long it waits. Existing `View()` and `ResolveSQLSource`
calls remain unchanged and fail fast when no snapshot is published, so this is
backward-compatible and default-off at the integration layer.

The readiness signal is one channel and one `sync.Once` per coordinator. The
steady-state fast path performs no allocation. Authorization, source limits,
checkpoint validation, and atomic publication still happen in
`CaptureWithCheckpoint`; `WaitReady` cannot bypass them.

## Measurement

Command: `make benchmark-round55-readiness`.

Five samples on Linux/amd64 with an AMD Ryzen 9 5950X compare the previous
published-view check with the new ready fast path:

```text
baseline_view:       3.726  3.757  3.746  3.756  3.773 ns/op; 0 B/op; 0 allocs/op
wait_ready_fast_path:3.948  3.849  3.723  3.715  3.706 ns/op; 0 B/op; 0 allocs/op
```

| Path | Median time | Heap | Allocations | Relative |
| --- | ---: | ---: | ---: | ---: |
| Existing `View()` readiness check | 3.756 ns | 0 B/op | 0 | 1.00x |
| `WaitReady(context.Background())` fast path | 3.723 ns | 0 B/op | 0 | 1.01x, within benchmark noise |

The benchmark demonstrates negligible steady-state cost; the primary gain is
bounded cancellation and correct admission before initial publication.
