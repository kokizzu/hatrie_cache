# C238: Mutation Queue Progress

The opt-in `hatSql.MutationController` now exposes operational estimates in
`MutationSnapshot`:

- `Remaining` is `Total - Completed` while a running mutation has a known
  total. Queued and terminal mutations report zero future work.
- `Elapsed` is the running time from `StartedAt` to the observation time, or
  from `StartedAt` to `FinishedAt` for a terminal snapshot.
- `EstimatedRemaining` uses the completed-work rate. It is zero until a
  positive completed count and known total are available, and it is always
  zero for terminal snapshots.

The estimate is best effort and should be treated as a progress signal, not a
deadline. It is calculated when `Snapshot` is read; progress reporting does
not gain a timer, goroutine, allocation, or additional per-job state.

## Example

```go
snapshot, err := handle.Snapshot()
if err != nil {
    return err
}
fmt.Printf("%d/%d rows, %d remaining, elapsed=%s, eta=%s\n",
    snapshot.Completed,
    snapshot.Total,
    snapshot.Remaining,
    snapshot.Elapsed,
    snapshot.EstimatedRemaining,
)
```

## Measurement

The paired benchmark is `BenchmarkC238MutationSnapshot` on Linux amd64,
AMD Ryzen 9 5950X, with `-benchmem -count=5`.

| Path | Raw ns/op | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: |
| Before derived progress fields | 46.65, 46.82, 47.70, 48.38, 49.49 | 0 | 0 |
| After derived progress fields | 100.2, 101.3, 102.8, 104.2, 106.2 | 0 | 0 |

The snapshot poll is about 2.15x slower, or roughly 55 ns more in this small
fixture. `MutationController` submit/wait remained at 446 B and 7 allocations
per operation; its five-run median moved from 2,449 ns/op to 2,651 ns/op amid
normal scheduler noise. The controller is already opt-in, and the added work
is confined to status reads, so the feature is retained with this explicit
diagnostic tradeoff.

Run the focused checks with:

```text
make test-c238
make benchmark-c238
```
