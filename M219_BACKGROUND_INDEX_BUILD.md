# M219: Background Index Creation With Observable Build Frontier

## Scope

M219 adds an opt-in asynchronous path for creating materialized-view point
postings after the view snapshot has already been published. It uses the
existing `hatSql.SQLIndexRebuildQueue`, so callers get bounded admission,
priority ordering, cancellation, history, and worker control without adding a
second queue implementation.

The existing synchronous behavior is unchanged. A view created with
`MaterializedViewDefinition.PointLookupFields` still builds its postings during
`MaterializedViews.Create` and refresh. Background creation is for callers that
want to publish the snapshot first and move index construction out of the
request or refresh critical path.

## API

```go
queue, err := hatSql.NewSQLIndexRebuildQueue(hatSql.SQLIndexRebuildQueueOptions{
	Workers: 2,
})
if err != nil {
	return err
}
defer queue.Close()

view, err := views.Create(ctx, hatSql.MaterializedViewDefinition{
	Name:         "people_projection",
	Query:        "FROM CACHE('people') AS p SELECT p.id, p.region, p.name",
	Dependencies: []string{"people"},
}, resolver, hatSql.QueryOptions{})
if err != nil {
	return err
}
_ = view

status, err := views.EnqueuePointLookupBuild(queue,
	hatSql.MaterializedViewPointLookupBuildRequest{
		ID:       "people-region-v1",
		ViewName: "people_projection",
		Fields:   []string{"region"},
		Priority: 10,
	})
if err != nil {
	return err
}

if err := queue.Start(ctx); err != nil {
	return err
}

for {
	current, ok := queue.Status(status.ID)
	if !ok {
		return fmt.Errorf("build status disappeared")
	}
	log.Printf("state=%s completed=%d/%d frontier=%d",
		current.State, current.Completed, current.Total, current.Frontier)
	if current.State == hatSql.SQLIndexRebuildSucceeded {
		break
	}
	if current.State == hatSql.SQLIndexRebuildFailed ||
		current.State == hatSql.SQLIndexRebuildCanceled {
		return errors.New(current.Error)
	}
	// Use the application's normal event loop or a bounded polling interval.
}
```

`Fields` can be omitted when the view definition already contains
`PointLookupFields`. The request ID must be unique in the queue history. A
request is accepted before `Start`, which allows an application to enqueue a
batch and then start workers once its service is ready.

## Frontier Semantics

The queue reports two related progress values:

- `Completed` and `Total` are the generic integer progress values used by the
  existing rebuild queue.
- `Frontier` is the exclusive row ordinal incorporated by this row-oriented
  builder. A frontier of `256` means rows `[0, 256)` have been processed into
  the private posting map. It is monotonic and remains available in queue
  history after completion.

The builder reports progress at the start and after each 256-row batch. The
private posting map is never exposed as partially complete state, so a reader
continues using the old unindexed snapshot while the frontier advances.

## Publication And Stale Work

The task captures the published snapshot revision and the original configured
point fields before it starts. It builds postings from that immutable result
without holding the registry lock. Publication then takes one short exclusive
lock and succeeds only if:

1. the view still exists;
2. its snapshot revision is unchanged; and
3. its configured point fields are unchanged.

If any condition fails, the task is marked failed and the private postings are
discarded. Cancellation and build errors also discard the private map. There is
therefore no window where a reader can observe half-built postings or postings
from an older snapshot. A successful request installs the postings and the
requested fields together.

After a successful request, later refreshes use the configured fields through
the existing synchronous refresh path. If a refresh wins the race first, the
background request fails instead of overwriting the fresh result.

## Capacity And Operations

Queue capacity and worker count remain application choices. A full queue
returns `ErrSQLIndexRebuildQueueFull`, so callers should use bounded retry or
backpressure rather than an unbounded submission loop. `Priority` is honored
by the existing queue. `Cancel` stops queued work immediately and asks running
work to stop through its context. `Flush` waits for the queue to become idle.

The background path is intentionally opt-in because it retains a private
posting map for the duration of a build and can briefly increase heap usage.
Use a small worker count when query latency is more important than catch-up
time, and a larger count only when the host has CPU and memory headroom.

## Measurement

The benchmark uses a 4,096-row already-published snapshot and a single
`region` point field. The synchronous `Create` cases include source resolution,
snapshot construction, and publication. The background build case starts with
the snapshot already published, so its total is not an apples-to-apples
replacement for `Create`; it measures the work moved out of the caller path.
The enqueue case measures only submission and status allocation.

On Linux/amd64 with an AMD Ryzen 9 5950X:

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Create without point index | 6,386,904 | 5,959,650 | 57,022 |
| Create with synchronous point index | 6,578,477 | 6,101,268 | 61,581 |
| Background enqueue only | 1,008 | 819 | 8 |
| Background build and flush | 408,266 | 142,902 | 4,572 |

Relative to the no-index `Create` baseline, synchronous point postings add
about 3.0% CPU, 2.4% transient bytes, and 8.0% allocations in this run. M219
does not claim to remove that total work; it makes submission cheap and moves
the posting build behind a queue while preserving atomic read behavior. Full
raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#m219-background-index-creation).

## Verification

The focused M219 tests cover:

- readable snapshots before publication and point-planner use after publication;
- exact `Frontier == Total` completion;
- stale revision rejection without index publication; and
- unknown-field rejection.

Run the focused gates with:

```text
make m219-test
make m219-race
make m219-vet
make m219-benchmark
```
