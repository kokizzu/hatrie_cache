# Mutation Dependency Graph

`hatPipeline.MutationDependencyGraph` is an importable, caller-driven
coordinator for overlapping maintenance or data mutations. It records which
work is safe to run, makes failed prerequisites visible, and lets a caller
resume after restoring an explicit snapshot. It does not start goroutines or
execute user work by itself.

## Why It Exists

Maintenance jobs often overlap: an index rebuild may depend on a schema change,
a projection refresh may depend on a source checkpoint, and a compaction may
depend on a completed mutation. Re-scanning every job on every poll wastes CPU,
while blindly running a dependent job can observe incomplete state.

The graph maintains reverse dependent lists and an O(1) ready-set update when a
dependency completes. `Ready` sorts only the currently runnable IDs, so a
large graph with a small ready set does not scan every node on each poll.

## Lifecycle

1. Add dependencies after adding their prerequisite nodes. The API rejects
   unknown IDs, duplicate dependencies, duplicate nodes, and self-cycles.
2. Poll `Ready` or `ReadyInto` and call `Start` for the work the caller owns.
3. Call `Complete(id, nil)` on success. Dependents whose prerequisites are all
   complete become ready immediately.
4. Call `Complete(id, err)` on failure. The failed node is retryable and its
   dependents report `blocked` until the failure is retried successfully.
5. Call `Retry` after correcting the failed operation. A successful retry
   returns the node to the ready set.

The graph is safe for concurrent callers. It tracks metadata only; callers
must provide their own execution, cancellation, lease policy, and durable
checkpoint cadence.

## States

| State | Meaning |
| --- | --- |
| `pending` | Waiting for unfinished prerequisites. |
| `ready` | All prerequisites completed; safe to start. |
| `running` | Claimed by a caller through `Start`. |
| `completed` | Finished successfully. |
| `failed` | Finished with an error; `Retry` may make it ready again. |
| `blocked` | A prerequisite is failed or transitively blocked. |

`MutationStatus.Remaining` reports unfinished direct prerequisites. `Statuses`
and `Snapshot` return deterministic ID order and copies of dependency slices.

## Checkpoint And Recovery

`Snapshot` is an in-memory value intended to be stored by the caller. It
contains IDs, dependencies, states, and failure text. The restore function
validates all IDs, references, duplicate dependencies, states, and cycles.

Running work is converted to pending on restore so an interrupted operation is
resumable rather than silently considered complete. Ready and blocked states
are recomputed from the restored dependency states. A snapshot with more than
one million nodes is rejected before allocation.

The graph has no implicit disk I/O and no automatic retry. This keeps the
durability boundary explicit and avoids coupling the coordinator to a storage
format or a transaction protocol.

## Example

```go
graph := hatPipeline.NewMutationDependencyGraph()
_ = graph.Add("index")
_ = graph.Add("projection", "index")

fmt.Println(graph.Ready()) // [index]
_ = graph.Start("index")
_ = graph.Complete("index", nil)
fmt.Println(graph.Ready()) // [projection]
```

## Measurement

The focused benchmark builds a 4,096-node dependency chain with one ready
node. It compares the indexed ready set with a control that scans all nodes on
each poll. Five samples were run on the local AMD Ryzen 9 5950X host:

| Poll implementation | Raw samples (ns/op) | Median ns/op | B/op | allocs/op | Relative |
| --- | --- | ---: | ---: | ---: | ---: |
| Ready-set index | 46.60, 49.04, 43.83, 42.01, 47.92 | 46.60 | 0 | 0 | 1.00x |
| Full node scan control | 41565, 40260, 41486, 44242, 40438 | 41486 | 0 | 0 | 890.3x slower |

Command:

```text
make benchmark-ch014-c302
```

This is a polling benchmark, not a claim about end-to-end mutation duration.
The indexed path still sorts the ready IDs for deterministic scheduling; a
caller that needs one item can use `ReadyInto(buffer, 1)` and reuse its buffer.
