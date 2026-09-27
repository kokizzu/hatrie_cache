# CH-006: Durable Mutation Queue Worker

`SQLMutationDependencyQueue` already provides durable dependency-aware task
transitions. `RunReady` adds the small worker loop that callers otherwise had
to duplicate:

```go
stats, err := queue.RunReady(ctx, 32, func(ctx context.Context, task hatSql.SQLMutationTaskRecord) error {
	return applyMutation(ctx, task.ID)
})
```

The zero or negative limit processes the whole ready batch. Tasks are claimed
in deterministic ID order. A successful callback is durably completed. A
callback error, or cancellation observed between claimed tasks, is durably
marked failed and can later be retried with `Retry`. The method finishes the
rest of the already-claimed batch and returns the first error after all
transitions have been attempted. It does not execute SQL implicitly; the
callback remains the application's `ALTER`, `DELETE`, index, or compaction
operation.

The default queue, graph, and SQL executor behavior are unchanged. The worker
is opt-in and remains local to the queue's existing durability and lease
semantics. It is a bounded execution convenience, not distributed fencing or
automatic SQL planner wiring.

## Measurement

The benchmark creates 64 independent ready tasks, excludes queue setup from
the timed interval, and performs one durable completion per task. Each result
therefore includes the same filesystem sync workload. Each path runs in a
separate process, with nine one-batch samples; the worker path runs first to
reduce cross-benchmark disk queue contention. Machine: AMD Ryzen 9 5950X,
Linux, `-benchtime=1x -count=9`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Manual `ClaimReady` + `Complete` control | 42,961,396 | 17,856 | 339 | 1.000x |
| `RunReady` | 41,867,746 | 17,856 | 339 | 0.975x |

The initial pre-change manual baseline had a 44,454,890 ns/op median, with one
277,457,547 ns/op filesystem outlier. The controlled post-change samples still
ranged from 39.7 ms to 48.3 ms for `RunReady` and 39.7 ms to 52.7 ms for the
manual control. The 2.5% median difference is within that filesystem variance,
so this is not presented as a speed improvement. The important result is no
material CPU, memory, or allocation regression while removing repeated worker
state code and standardizing failure handling.

Raw post-change samples:

```text
RunReady: 41711152 17856 339
RunReady: 41867746 17856 339
RunReady: 48293481 17904 341
RunReady: 41574579 17856 339
RunReady: 42187985 17856 339
RunReady: 47852379 17920 341
RunReady: 39748789 17856 339
RunReady: 43078939 17880 340
RunReady: 39851121 17856 339

Manual: 42960416 17856 339
Manual: 52742991 17904 341
Manual: 43613114 17856 339
Manual: 43391928 17880 340
Manual: 42961396 17880 340
Manual: 41332612 17856 339
Manual: 40125499 17856 339
Manual: 43222453 17880 340
Manual: 39652946 17856 339
```

Run the focused checks with:

```text
make test-ch006-mutation-worker
make benchmark-ch006-mutation-worker
make verify-ch006-mutation-worker
```
