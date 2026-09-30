# M235 Maintained-Object Dependency Invalidation

M235 adds `hatSql.SQLDependencyGraph`, a reusable reverse dependency index for
maintained SQL views and secondary indexes. A source mutation can ask for only
the objects that depend on the changed source instead of scanning every
maintained object.

## Example

```go
graph := hatSql.NewSQLDependencyGraph()
if err := graph.Register(
    hatSql.SQLMaintainedObjectView,
    "orders_view",
    "orders",
    "customers",
); err != nil {
    return err
}
if err := graph.Register(
    hatSql.SQLMaintainedObjectIndex,
    "orders_by_email",
    "orders",
); err != nil {
    return err
}

affected := graph.Affected([]string{"orders"})
for _, object := range affected {
    switch object.Kind {
    case hatSql.SQLMaintainedObjectIndex:
        rebuildOrUpdateIndex(object.Name)
    case hatSql.SQLMaintainedObjectView:
        refreshView(object.Name)
    }
}
```

`Affected` returns detached objects in deterministic kind/name order. Use
`AffectedInto` with a reusable destination slice in a polling loop. Registering
the same kind/name twice is rejected; unregister before replacing a
definition. Dependencies are trimmed, sorted, and required to be unique.

## Safety and scope

- Only `view` and `index` object kinds are accepted, so accidental unrelated
  catalog entries do not silently enter the invalidation path.
- Each object is limited to 256 source dependencies.
- Reverse-index updates are atomic and all methods are safe for concurrent
  callers.
- Multi-source changes are deduplicated, so an object depending on two changed
  sources is returned once.
- The graph does not execute refreshes, rebuild indexes, persist state, or
  change SQL planner defaults. Callers own those actions and can keep the
  feature disabled by never registering a graph.
- Existing `MaterializedViews` behavior remains unchanged. This graph is the
  importable common catalog for callers that maintain both views and indexes.

## Measurement

Five 200 ms samples were collected on an AMD Ryzen 9 5950X,
`linux/amd64`, with 8,192 registered objects and 8 or 9 affected objects.
The baseline scans every object and checks its dependency list. Lower is
better.

| Workload | Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| one changed source | Linear scan | 15,572 | 128 | 8 | 1.00x |
| one changed source | Indexed graph | 767 | 152 | 9 | 0.05x |
| two changed sources | Linear scan | 26,394 | 144 | 9 | 1.00x |
| two changed sources | Indexed graph | 1,195 | 480 | 12 | 0.05x |

The indexed graph is about 20.3x faster for one source and 22.1x faster for
two sources. The single-source path adds only 24 B/op and one allocation. The
two-source path allocates a temporary candidate-key slice for deduplication;
it still uses 5.0% of the scan CPU, while its transient bytes are 3.3x the
scan because it returns detached metadata. This cost is bounded by the number
of affected registrations, not the total catalog size.

Raw samples are in [`M235_BENCHMARK_RAW.txt`](M235_BENCHMARK_RAW.txt).
Reproduce the focused checks with:

```text
make test-m235-red
make benchmark-m235
```
