# CH-U39 Workload Admission Priorities

Namespace query limits already bound concurrency and queue size, but a FIFO
queue cannot express that interactive work should run before background work.
This feature adds an opt-in workload priority hint to namespace-governed SQL
execution.

## API

Set `SQLQueryOptions.WorkloadPriority` when calling
`NamespaceQueryGovernor.Execute`:

```go
result, err := governor.Execute(
	ctx,
	"tenant-eu",
	"SELECT value FROM CACHE('events')",
	resolver,
	nil,
	hatSql.SQLQueryOptions{WorkloadPriority: 100},
)
```

Higher values are preferred while queries are waiting for a namespace
concurrency slot. A value of zero is the default and preserves FIFO behavior.
Direct SQL execution and callers that do not use `NamespaceQueryGovernor` are
unchanged.

## Scheduling Rules

- Priorities are clamped to `-1,048,576` through `1,048,576` so callers cannot
  overflow the scheduler score.
- The first release selects the highest `priority + skipped` score.
- Every waiter that is bypassed gains one aging point. This guarantees that a
  continuously busy queue cannot indefinitely starve an older low-priority
  query, while still allowing a meaningful priority difference.
- Ties are resolved by arrival sequence, so scheduling remains deterministic.
- Context cancellation removes a queued waiter and does not consume a slot.
- The normal uncontended path remains allocation-free. Priority scheduling is
  only relevant when a query is queued.

Priority is a scheduling hint, not a resource-limit bypass: row, byte, timeout,
quota, and cancellation enforcement still apply to every query.

## Benchmark

The benchmark was run on an AMD Ryzen 9 5950X with five samples per case. All
cases used `0 B/op` and `0 allocs/op`.

| Case | Median ns/op | Relative to same-run control | Meaning |
| --- | ---: | ---: | --- |
| Pre-feature clean FIFO run | 8.591 | 0.89x | Separate clean-base reference |
| Same-run FIFO control | 9.621 | 1.00x | Existing `acquire` fast path |
| `WorkloadPriority=0` | 9.627 | 1.00x | Default path after fast-path preservation |
| `WorkloadPriority=10` | 10.44 | 1.09x | Opt-in priority path without waiters |
| Priority selection, 2 waiters | 15.78 | 1.64x | Queued selection cost |
| Priority selection, 8 waiters | 34.68 | 3.60x | Queued selection cost |
| Priority selection, 64 waiters | 230.2 | 23.93x | Queued selection cost |

The pre-feature row is a separate run and is included for historical context;
the same-run control is the relevant comparison. The zero-priority path is
effectively unchanged from that control. Queued selection is an O(waiters)
scan, intentionally trading a small scheduling cost for priority ordering and
starvation protection. It performs no heap allocation. The feature does not
claim a raw throughput improvement; its benefit is predictable workload
fairness under contention.

Raw samples (`ns/op`, in collection order):

```text
pre-feature FIFO: 8.578 8.591 8.983 9.054 8.557
same-run FIFO control: 9.540 10.01 9.621 9.643 9.529
priority=0: 9.412 9.736 9.627 9.471 10.09
priority=10: 10.97 10.06 10.38 10.44 10.45
selection waiters=2: 16.42 15.70 16.51 15.78 15.48
selection waiters=8: 34.68 33.78 34.31 35.14 36.08
selection waiters=64: 230.2 227.4 228.7 232.4 231.8
```

Raw benchmark commands:

```text
make codex-chu45-with-ties-baseline
make codex-chu45-with-ties-benchmark
```

The temporary benchmark worktree and reports are removed after the commit.
