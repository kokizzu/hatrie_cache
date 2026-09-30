# M236 On-Demand Maintained-Object Refresh

M236 adds `hatSql.SQLMaintainedRefreshRegistry`, a caller-driven execution
layer over the bounded M235 dependency graph.

## Behavior

- `Register` associates one `view` or `index` with source dependencies and a
  refresh callback.
- `RefreshChanged` runs callbacks only for objects affected by the changed
  source names.
- `RefreshChangedInto` reuses a caller-owned result buffer for lower steady-
  state allocation.
- Callbacks run in deterministic kind/name order and receive detached object
  metadata, so callback mutation cannot alter the registry.
- A callback error stops the cycle and returns the objects completed before the
  error. Completed callbacks are not rolled back.
- `Unregister`, `Snapshot`, and `Len` support lifecycle and inspection.
- The registry is concurrent-safe and accepts a nil context as background
  context. It does not start goroutines or change existing materialized-view
  scheduler behavior.

The refresh callback owns the actual rebuild or publish operation. This keeps
backup, transaction, retry, and failure semantics in the component that owns
the maintained object instead of hiding them in a generic registry.

## Benchmark

Workload: 8,192 registered maintained objects, eight affected by one source,
no-op callbacks, five 200 ms samples on AMD Ryzen 9 5950X, linux/amd64.

| Path | Median ns/op | B/op | Allocs/op | Speed vs linear |
| --- | ---: | ---: | ---: | ---: |
| Linear scan and refresh | 12,795 | 0 | 0 | 1.00x |
| Indexed `RefreshChanged` | 1,353 | 1,000 | 13 | 9.46x |
| Indexed `RefreshChangedInto` with reused output | 986.4 | 152 | 9 | 12.97x |

The indexed path trades a small per-affected-object detached-metadata cost for
avoiding the full 8,192-object scan. The reusable API is the preferred path for
hot invalidation loops; the convenience API is simpler when allocation is not
on the critical path. Raw samples are in [M236_BENCHMARK_RAW.txt](M236_BENCHMARK_RAW.txt).
