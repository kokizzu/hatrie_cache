# M064 Recursive Dataflow Maintenance

This feature adds a small, generic fixed-point executor for recursive dataflow
operators. It follows the useful part of Materialize-style recursive
maintenance: each round receives only the delta discovered by the previous
round, so a recursive operator does not need to rescan the complete result for
every round. It is a reusable primitive in `hat/hatSql`; existing SQL plans do
not automatically switch to it.

## API

```go
flow, err := NewSQLRecursiveDataflow(
    []string{"root"},
    func(ctx context.Context, all, delta []string) ([]string, error) {
        next := make([]string, 0, len(delta))
        for _, source := range delta {
            next = append(next, edges[source]...)
        }
        return next, nil
    },
    SQLRecursiveDataflowOptions{},
)
rows, err := flow.Run(ctx)
```

The executor:

- snapshots and de-duplicates the seed in first-seen order;
- passes the complete result as `all` and only newly discovered values as
  `delta`;
- de-duplicates candidates, preserves deterministic first-seen order, and
  terminates cycles;
- gives each `Run` call an independent result, so changing the returned slice
  does not change a later run;
- checks cancellation before and after every step; and
- fails closed on iteration and retained-row limits.

The step must treat `all` and `delta` as read-only. Its returned candidate
slice is transferred to the executor and must not alias either input slice.
Steps that need to retain or reuse input storage should copy it first.

## Bounds

Zero-valued options use bounded defaults:

| Option | Default | Hard maximum |
| --- | ---: | ---: |
| `MaxIterations` | 1,024 | 1,048,576 |
| `MaxRows` | 1,048,576 | 16,777,216 |

The bounds prevent a malformed or adversarial recursive step from running
forever or retaining unbounded state. A caller can choose smaller limits for a
query-specific budget. This primitive is not a replacement for a SQL planner
or a signed differential arrangement: arbitrary deletes and updates still
belong to the existing specialized maintainers or a rebuild path.

## Measurement

The workload is a 1,024-node directed graph where every node points to the
next one and two nodes. It compares a full-frontier scan that rescans every
discovered node each round with the delta-driven executor. The graph setup is
outside the timed loop. Three samples were run on Linux/amd64 with the
repository benchmark target.

| Path | Median ns/op | Median B/op | Median allocs/op |
| --- | ---: | ---: | ---: |
| Full-frontier scan before implementation | 5,002,857 | 107,644 | 1,063 |
| Delta-driven executor | 125,060 | 111,632 | 545 |

The delta path is approximately 40.0x faster and makes 48.7% fewer
allocations, at the cost of 3.7% more transient bytes per operation in this
workload. The cost is accepted because the CPU win is large and the retained
result remains bounded. The raw samples are recorded in
[`BENCHMARK.md`](BENCHMARK.md).

Run the focused checks with:

```text
make test-m064-recursive-dataflow
make test-m064-sql-package
make race-m064-recursive-dataflow
make vet-m064-recursive-dataflow
```
