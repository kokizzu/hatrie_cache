# M052 Dataflow Topological-Order Cache

`hatPipeline.DataflowGraph` now caches the deterministic result of
`TopologicalOrder` until a graph mutation occurs. This is a Materialize-style
optimization for planners and explainers that ask for the same immutable graph
order repeatedly while a plan is being inspected or executed.

## Behavior

- The first call uses the existing Kahn sort and stores its detached result.
- Repeated calls return a fresh slice, so callers can mutate their result
  without changing the graph or a later result.
- `AddNode`, `AddEdge`, `RemoveEdge`, and `RemoveNode` invalidate the cache.
- The cache is protected by the graph's existing mutex and is safe with
  concurrent readers and mutations.
- Cycle checks, deterministic lexical ordering, limits, snapshots, impact
  queries, and all existing errors are unchanged.

No new option is required and no worker starts. The cache is internal and
automatically enabled for every graph. The retained cache stores only the
ordered string slice; it reuses the existing node-ID string values rather than
copying them. The returned detached slice still costs one allocation per
caller, preserving the previous ownership contract.

## Measurement

Five `-benchtime=100ms -count=5` samples were collected on Linux/amd64 with
an AMD Ryzen 9 5950X. The benchmark builds a 2,048-node, 2,047-edge chain
once, then repeatedly asks for its topological order.

| Workload | Before | After | Improvement | Before memory | After memory | Before allocs | After allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Repeated `TopologicalOrder` | 369,834 ns/op | 9,479 ns/op | 39.0x faster | 174,752 B/op | 32,785 B/op | 11 | 1 |

Raw before samples:

```text
376467 373637 369834 368043 368238 ns/op, 174752 B/op, 11 allocs/op
```

Raw after samples:

```text
9763 9479 9115 9854 8758 ns/op, 32785/32785/32782/32785/32785 B/op, 1 alloc/op
```

The first call after a mutation still pays the original sort cost, while the
next call is cached. The invalidation assignment is constant-time and does not
alter the default graph mutation semantics.
