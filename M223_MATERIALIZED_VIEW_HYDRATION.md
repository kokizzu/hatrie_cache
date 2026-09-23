# M223: Materialized View Hydration States

M223 adds an explicit lifecycle state for optional materialized-view point
indexes. Callers can distinguish a view that has no maintained index (`cold`),
one whose index build is queued or running (`hydrating`), and one whose
declared point indexes are fully published (`ready`).

## API

```go
status, ok := views.HydrationStatus("people_projection")
```

`MaterializedViewHydrationStatus` contains the view name, snapshot revision,
declared index fields, current state, and the background rebuild task ID when a
task owns the hydration transition. The state constants are:

- `MaterializedViewHydrationCold`
- `MaterializedViewHydrationHydrating`
- `MaterializedViewHydrationReady`

The API is additive. `PointLookup` and ordinary SQL execution keep their
existing behavior: while a view is cold or hydrating, the planner can use the
existing arrangement-scan fallback.

## State Transitions

- A view created or refreshed without point postings is `cold`.
- A view created or synchronously refreshed with all declared postings is
  `ready`.
- `EnqueuePointLookupBuild` publishes `hydrating` as soon as the queue accepts
  the task, including the queued-before-worker-start interval.
- Successful publication changes the state to `ready`.
- Failed or canceled builds return to the currently published index state;
  they do not leave the view permanently `hydrating`.
- Removing all postings returns the view to `cold`; removing only some fields
  leaves it `ready` when every remaining field still has postings.

Refreshes, drops, and newer builds invalidate older task ownership. A build
can publish only while its task ID, snapshot revision, and original field
generation still match. This prevents a stale worker from resurrecting an
index after an operator removes it.

## Cost And Verification

Hydration metadata is stored in a registry side map, so the hot `PointLookup`
view record does not grow. `HydrationStatus` is a control-plane call and clones
the field list for caller isolation. The benchmark records its small
allocation cost separately from point lookup throughput in
[BENCHMARK.md](BENCHMARK.md#m223-materialized-view-hydration-states).

Focused verification:

```text
make m223-test
make m223-race
make m223-vet
make m223-benchmark
```
