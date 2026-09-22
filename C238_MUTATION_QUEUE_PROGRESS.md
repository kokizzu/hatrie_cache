# C238 Mutation Queue Progress

C238 adds a zero-allocation progress snapshot to the SQL mutation dependency
graph and its durable queue. It reports `total`, `ready`, `pending`,
`blocked`, `running`, `completed`, `failed`, and `remaining` task counts.
`ready` is a subset of `pending`; `blocked` is pending work waiting on an
unfinished dependency. `remaining` is `total - completed`, so failed tasks
remain visible until they are retried or otherwise resolved.

The durable queue also reports:

- `elapsed_nanos`: time since the current queue instance was opened;
- `estimated_remaining_nanos`: a best-effort estimate based on completed tasks
  per elapsed time and the current remaining count.

The estimate is intentionally operational rather than durable. Reopening a
queue starts a new timing window, and the estimate is omitted until at least
one task has completed. A clock moving backwards is clamped to zero elapsed
time.

Example:

```json
{
  "total": 100,
  "ready": 8,
  "pending": 90,
  "running": 2,
  "completed": 8,
  "failed": 0,
  "blocked": 82,
  "remaining": 92,
  "elapsed_nanos": 20000000000,
  "estimated_remaining_nanos": 230000000000
}
```

`SQLMutationDependencyGraph.Progress()` reads the graph under its existing
read lock without cloning task records. `SQLMutationDependencyQueue.Progress()`
adds the timing fields while preserving the queue's serialized access model.

## Verification

```text
make test-c238-progress
make race-c238-progress
make vet-c238-progress
make benchmark-c238-before-after
```
