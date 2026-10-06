# M052 Dataflow Snapshot Cache

`hatPipeline.DataflowGraph` now caches its deterministic sorted node and edge
snapshot until a graph mutation occurs. This complements the topological-order
cache and targets Materialize-style planners and explainers that repeatedly
inspect an unchanged dataflow graph.

## Behavior

- The first call still builds the snapshot from the graph maps and sorts IDs.
- Repeated calls return detached node and edge slices; caller mutation cannot
  change the graph or a later snapshot.
- `AddNode`, `AddEdge`, `RemoveEdge`, and `RemoveNode` invalidate the snapshot
  and the topological-order cache together.
- The cache uses the existing graph mutex and remains safe during concurrent
  reads and mutations.
- Graphs larger than the default node or edge bound do not retain a snapshot;
  they keep the previous rebuild behavior to avoid an unbounded derived-memory
  cost. A normal graph is bounded by `DefaultDataflowGraphMaxNodes` and
  `DefaultDataflowGraphMaxEdges`.

The API, JSON shape, ordering, error behavior, and mutation semantics are
unchanged. No worker or configuration flag is added. Cached strings reuse the
graph's existing ID values; only the bounded node/edge slice backing arrays
are retained. A detached slice is still copied for each caller.

## Measurement

Five `-benchmem -count=5` samples were collected on Linux/amd64 with an AMD
Ryzen 9 5950X. The retained benchmark fixture builds a 2,048-node,
2,047-edge chain once, then repeatedly calls `Snapshot`. The same fixture and
benchmark are run before and after the cache is adopted.

| Workload | Before | After | Improvement | Before memory | After memory | Before allocs | After allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Repeated `Snapshot` | 463,408 ns/op | 34,821 ns/op | 13.3x faster | 163,840 B/op | 131,076 B/op | 3 | 2 |

Raw before samples:

```text
464953 463408 469896 454892 459587 ns/op, 163842/163840/163840/163840/163840 B/op, 3 allocs/op
```

Raw after samples:

```text
34543 34821 35133 34855 34079 ns/op, 131077/131077/131076/131076/131076 B/op, 2 allocs/op
```

The cache trades a bounded retained snapshot for substantially lower repeated
CPU and transient allocation cost. Mutations release both derived caches, and
the size guard disables retention above the default graph bounds.
