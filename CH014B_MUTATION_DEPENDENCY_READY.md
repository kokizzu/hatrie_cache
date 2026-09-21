# CH-014b Mutation Dependency Ready Queue

`SQLMutationDependencyGraph.ClaimReady` now uses an in-memory reverse
dependency index and a deterministic min-heap of ready task IDs. The durable
snapshot and mutation-queue record formats are unchanged.

## Behavior

- `Add` records reverse edges and enqueues a task when all of its dependencies
  are already complete.
- `Complete` only revisits direct dependents, so unrelated tasks are not
  scanned.
- `Retry` and crash-recovery `RequeueRunning` restore eligible tasks to the
  ready heap.
- `Load` rebuilds the reverse index and ready heap from the validated snapshot.
- Ready tasks are still claimed in lexical ID order, and claim rollback
  re-enqueues tasks when a durable queue append fails.
- An empty `ClaimReady` poll returns immediately without allocating or
  scanning every task.

The optimization is in-memory only. Existing JSON graph snapshots, CRC-
protected mutation queue records, restart replay, and public task state
semantics remain compatible.

## Measurement

Commands:

```text
make benchmark-ch014b-mutation-dependency-graph-baseline
make benchmark-ch014b-mutation-dependency-graph
```

The comparable microbenchmark uses 4,096 blocked tasks, one running root, and
repeated `ClaimReady(1)` calls with `-cpu=1`, `-count=5`. The legacy control
implements the previous full task-map scan; the optimized path uses the ready
heap.

| Path | Median ns/op | B/op | allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Legacy full scan control | 156,676 | 73,728 | 1 | 18,954x slower |
| Reverse index + ready heap | 8.266 | 0 | 0 | 1.00x |

Raw legacy samples were `146,447`, `157,081`, `156,676`, `153,416`, and
`158,276` ns/op. Raw optimized samples were `8.154`, `8.729`, `8.018`,
`8.266`, and `8.556`
ns/op. The result is specifically for the empty-ready polling workload; it
does not claim that every mutation-graph operation is 19,226x faster.

## Tradeoff

The graph retains one reverse-edge string reference per dependency and a heap
entry for each currently ready task. Adding or completing a task now performs
index maintenance and heap work, typically `O(log R)` for `R` ready tasks.
This is a deliberate exchange for making repeated empty polls `O(1)` and
making completion wake only direct dependents instead of scanning `O(N)` tasks.
The queue remains opt-in and no SQL executor default is changed.
