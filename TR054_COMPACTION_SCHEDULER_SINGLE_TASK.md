# TR-054 Single-Task Compaction Scheduler Fast Path

The compaction scheduler already coalesces requests and bounds concurrent
maintenance. This follow-up applies a narrow Tarantool/Vinyl-style maintenance
optimization: when a drain contains exactly one task, `Run` executes it
directly and skips sorting, worker-goroutine startup, the error array, and the
wait group used by the multi-task path.

The existing multi-task path is unchanged in structure. Failed singleton tasks
retain the existing retry behavior, and callbacks may still schedule a
different task while the current task is running. The callback runs on the
goroutine calling `Run` only for the singleton case; `Run` remains synchronous.

## Measurement

The benchmark uses Linux/amd64 on an AMD Ryzen 9 5950X. Each result has three
samples from `make benchmark-compaction-fastpath-c207`. The benchmark compiles
the scheduler source and focused tests directly so it remains reproducible
while unrelated packages are being changed concurrently.

| Workload | Before median | After median | Relative result | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 task | 1,083 ns | 278.7 ns | 3.89x faster | 184 | 40 | 7 | 2 |
| 4 tasks | 2,586 ns | 2,596 ns | 1.00x, 0.4% slower | 616 | 616 | 12 | 12 |
| 64 tasks | 19,178 ns | 19,028 ns | 1.01x faster | 3,400 | 3,400 | 12 | 12 |

Raw before samples:

```text
tasks-1: 1083 1143 1041 ns/op; 184 B/op; 7 allocs/op
tasks-4: 2635 2572 2586 ns/op; 616 B/op; 12 allocs/op
tasks-64: 17978 19496 19178 ns/op; 3400 B/op; 12 allocs/op
```

Raw after samples:

```text
tasks-1: 283.1 270.2 278.7 ns/op; 40 B/op; 2 allocs/op
tasks-4: 2633 2596 2588 ns/op; 616 B/op; 12 allocs/op
tasks-64: 19028 19141 18562 ns/op; 3400 B/op; 12 allocs/op
```

Correctness coverage includes singleton follow-up scheduling, failed-task
retry/requeue, `errors.Is` preservation, normal tests, race tests, and vet.
