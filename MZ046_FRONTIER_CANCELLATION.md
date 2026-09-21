# MZ-046 Frontier-Aware Cancellation

`hatPipeline.NewFrontierCancellation` creates an opt-in context that is
canceled when a named `FrontierRegistry` lower frontier reaches a requested
target. This lets a long-running query or pipeline stop after a freshness or
result frontier is satisfied without polling from the caller.

```go
guard, err := hatPipeline.NewFrontierCancellation(ctx, registry, "orders", 120)
if err != nil {
	return err
}
defer guard.Close()

err = runQuery(guard.Context())
if errors.Is(guard.Cause(), hatPipeline.ErrFrontierTargetReached) {
	// The requested freshness frontier was reached.
}
```

The constructor validates the registry and frontier ID synchronously. A
target already covered by the current frontier uses an immediate fast path
without starting a watcher. `Close` is idempotent; parent cancellation,
frontier removal, and registry closure are propagated through the returned
context. The helper is process-local and does not add persistence,
replication, or default query behavior.

## Measurements

Five 500 ms samples on Linux/amd64, AMD Ryzen 9 5950X with `-cpu=1`. The
active-path comparison creates a registry, waits for one frontier advancement,
and then waits for cancellation. The baseline uses `context.WithCancelCause`
so the cancellation-cause work is included on both sides.

| Path | Median time | Memory | Allocations | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Manual `WaitUntil` + cause-aware context | 976.2 ns/op | 680 B/op | 12 | 1.00x |
| `FrontierCancellation` | 1,064 ns/op | 672 B/op | 9 | 1.09x |
| Already-reached fast path | 238.3 ns/op | 160 B/op | 3 | n/a |

The active watcher costs 9% CPU in this controlled microbenchmark, reduces
allocation bytes by 1.2%, and reduces allocations by 25%. It is opt-in because
an active watcher intentionally uses one goroutine per guarded operation;
callers should always call `Close` when the operation finishes before the
frontier does.
