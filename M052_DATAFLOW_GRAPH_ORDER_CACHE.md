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

Five `-benchmem -count=5` samples were collected on Linux/amd64 with an AMD
Ryzen 9 5950X. The retained benchmark fixture builds a 2,048-node,
2,047-edge chain once, then repeatedly asks for its topological order. The
same fixture and benchmark are run before and after the cache is adopted.

| Workload | Before | After | Improvement | Before memory | After memory | Before allocs | After allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Repeated `TopologicalOrder` | 362,282 ns/op | 9,693 ns/op | 37.4x faster | 174,752 B/op | 32,769 B/op | 11 | 1 |

Raw before samples:

```text
362282 366807 361827 364450 360386 ns/op, 174752 B/op, 11 allocs/op
```

Raw after samples:

```text
9609 9361 9693 10353 11923 ns/op, 32769 B/op, 1 alloc/op
```

The first call after a mutation still pays the original sort cost, while the
next call is cached. The invalidation assignment is constant-time and does not
alter the default graph mutation semantics.
