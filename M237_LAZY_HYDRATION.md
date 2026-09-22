# M237 Lazy Materialized-View Hydration

`MaterializedViews.GetOrHydrate` provides an opt-in read path for maintained
views that were registered with `CreateCold`.

```go
view, err := views.GetOrHydrate(ctx, "people", resolver, hatSql.QueryOptions{})
if err != nil {
	return err
}
use(view.Result.Rows)
```

Behavior:

- A ready view is returned immediately, with the same independent snapshot
  copy semantics as `Get`.
- The first reader of a cold view starts hydration.
- Concurrent readers of that cold view wait for the same hydration and do not
  execute the source query again.
- A waiting reader can cancel its own wait through `ctx` without cancelling
  the hydration owner.
- If the owner fails or is cancelled, the view returns to cold and a later
  reader may retry.
- Existing `Get` remains non-blocking and existing explicit `Hydrate` calls
  still return `ErrMaterializedViewHydrationInProgress` for concurrent callers.

The shared wait state is removed when hydration completes, fails, or the view
is dropped. Dropping a view also wakes readers waiting on that hydration.

## Verification

Focused correctness and race tests cover single-owner hydration, shared
results, waiting-reader cancellation, cancelled cold reads, and the ready
fast path.

The steady-state benchmark uses five samples on an AMD Ryzen 9 5950X:

| Read path | Median | Memory | Allocs |
| --- | ---: | ---: | ---: |
| `Get` | 598.5 ns/op | 392 B/op | 5 |
| `GetOrHydrate` on ready view | 606.3 ns/op | 392 B/op | 5 |

The new path has the same allocation profile and about 1.01x the median
latency of `Get`, within normal benchmark noise. The one-time cold hydration
cost is the source query itself and is intentionally not hidden in the
steady-state comparison.

Run the checks with:

```text
make test-m237-lazy-hydration
make race-m237-lazy-hydration
make vet-m237-lazy-hydration
make benchmark-m237-lazy-hydration
```
