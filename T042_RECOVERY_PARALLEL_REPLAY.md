# T042 Recovery-Time Parallel Replay

Status: partial, opt-in library support.

`hatJournal.ParallelReplay` provides a bounded replay scheduler for callers
that have already decoded journal records and can assign each record to an
independent partition. Records in one partition stay ordered; different
partitions may run concurrently.

## Contract

- `ReplayTask` values must be in strictly increasing journal sequence order.
- Every task must have a non-nil `Apply` function.
- A partition is the caller-selected conflict domain. Do not put dependent
  mutations in different partitions.
- `Workers` set to `0` or `1` keeps the serial path. The default is therefore
  unchanged and does not allocate scheduler state.
- `Workers` above `1` is bounded by `MaxReplayWorkers` and is canceled after
  the first apply error or parent-context cancellation.
- All input validation completes before the first task is applied.
- An apply error does not roll back tasks that already completed successfully;
  callers must discard or recover that partial state according to their own
  journal contract.

Example:

```go
err := hatJournal.ParallelReplay(ctx, tasks, hatJournal.ReplayOptions{
	Workers: 4,
})
```

The helper is intentionally not enabled by the normal recovery path yet. The
parent cache package owns command semantics and must provide the correct
partition/conflict key before using it.

## Benchmark

Machine: AMD Ryzen 9 5950X, linux/amd64. Each result is the median of five
runs from `make benchmark-round34-journal`.

| Workload | Path | Median time | Memory | Relative time |
| --- | --- | ---: | ---: | ---: |
| 4,096 tasks, 64 partitions, CPU-heavy apply | direct serial baseline | 9.854 ms | 0 B, 0 allocs | 1.00x |
| Same workload | 4 workers | 3.180 ms | 42.8 KB, 90 allocs | 3.10x faster |
| 4,096 tasks, one partition | default serial | 9.807 ms | 0 B, 0 allocs | baseline |
| Same one-partition workload | 4 workers, serial fastpath | 9.045 ms | 0 B, 0 allocs | within benchmark noise |
| 4,096 tasks, 64 partitions, tiny apply | direct serial baseline | 11.137 us | 0 B, 0 allocs | 1.00x |
| Same tiny workload | 4 workers | 226.832 us | 42.7 KB, 90 allocs | 20.36x slower |

Parallel replay is consequently useful for expensive independent mutations,
not for trivial callbacks. The serial default prevents this scheduler overhead
from appearing in ordinary recovery.

## Verification

```text
make format-round34-journal
make test-round34-journal
make benchmark-round34-journal
```

The focused tests cover serial ordering, per-partition ordering, concurrent
independent partitions, validation-before-apply, apply errors, worker bounds,
and canceled contexts.
