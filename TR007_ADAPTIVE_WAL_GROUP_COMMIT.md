# TR-007: Adaptive WAL Group Commit

Status: adopted as an opt-in journal option. The default remains unchanged.

## What changed

`hatJournal.Options.AdaptiveGroupCommit` lets the existing durable group-commit
worker shorten its collection delay when the queue already shows writer
pressure. The worker still writes commands in order, performs one durability
sync for the collected batch, and applies or acknowledges successful commands
only after that sync.

The policy is deliberately small and bounded:

| Queue state when the worker wakes | Effective collection window |
| --- | --- |
| No queued jobs, zero window, or max batch of one | Configured window |
| Fewer than half of `GroupCommitMaxBatch` jobs queued | One quarter of the configured window |
| At least half of `GroupCommitMaxBatch` jobs queued | Zero; drain immediately |

This changes waiting time under load. It does not change journal record order,
rollback behavior, replay format, or the sync-before-apply/ack durability
boundary.

## Configuration

The field is false in the zero value and is disabled by default for existing
callers:

```go
options := hatCache.CommandJournalOptions{
	Format:              hatCache.DefaultCommandJournalFormat,
	GroupCommitWindow:   2 * time.Millisecond,
	GroupCommitMaxBatch: 64,
	AdaptiveGroupCommit: true,
}
journal, err := hatCache.OpenCommandJournalWithOptions(path, options)
```

Use it only after measuring the workload. A shorter window can reduce latency
for queued writers, but sparse traffic can produce smaller batches and more
syncs than the fixed-window path. Operators that need the previous behavior do
nothing; `AdaptiveGroupCommit: false` is equivalent to the prior implementation.

## Test-first verification

The focused test target was run before implementation and failed on the missing
option/helper as expected. After implementation, the following passed:

```text
make test-tr007
make race-tr007
make vet-tr007
make test-tr007-package
```

The tests cover the default-off option, queue-pressure thresholds, and the
existing journal package behavior.

## Benchmark

The benchmark uses 16 concurrent `SETSTR` callers, a 2 ms configured window,
`GroupCommitMaxBatch=64`, and a no-op sync hook. Each row is five
`-benchmem` samples on Linux/amd64. `syncs/round` counts durability syncs for
the complete 16-command round.

### Raw samples

| Path | ns/op samples | B/op samples | allocs/op samples | syncs/round |
| --- | --- | --- | --- | ---: |
| Pre-change fixed window | 2,190,102; 2,190,955; 2,186,356; 2,194,431; 2,193,167 | 16,343; 16,091; 16,117; 16,074; 16,127 | 90; 89; 90; 89; 89 | 1.000 |
| Final fixed control | 2,188,096; 2,188,399; 2,188,571; 2,187,286; 2,184,768 | 16,259; 16,117; 16,027; 16,073; 16,069 | 90; 89; 89; 89; 89 | 1.000 |
| Adaptive window | 1,996,152; 1,955,632; 1,939,148; 1,952,730; 1,972,774 | 16,082; 16,074; 16,038; 16,028; 16,051 | 89; 89; 89; 89; 89 | 1.000 |

### Median comparison

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Final fixed control | 2,188,096 | 16,073 | 89 | 1.00x |
| Adaptive window | 1,955,632 | 16,051 | 89 | 1.12x faster; 0.999x bytes; same allocations |

The final control and adaptive path both performed one sync per 16-command
round in this workload. The result is therefore a latency improvement without
a measured memory or allocation increase, but it is not evidence that every
workload will retain the same batching ratio. Re-run the benchmark with the
actual storage device and concurrency before enabling the option in production.

Benchmark commands:

```text
make baseline-tr007
make benchmark-tr007
```
