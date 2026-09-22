# C231 SQL Workload Groups

`hatSql.SQLWorkloadAdmission` already provided a controller-wide concurrency
limit and weighted priority classes. C231 extends those classes with optional
per-class concurrency and memory budgets.

## Configuration

```go
admission, err := hatSql.NewSQLWorkloadAdmission(hatSql.SQLWorkloadAdmissionOptions{
    MaxConcurrent: 8,
    Classes: []hatSql.SQLWorkloadClass{
        {
            Name:           "analytics",
            MaxConcurrent:  2,
            MaxMemoryBytes: 256 << 20,
        },
        {Name: "interactive", MaxConcurrent: 6},
    },
})
if err != nil {
    return err
}
defer admission.Close()
```

- `MaxConcurrent` on a class is an additional cap; the controller-wide cap
  still applies.
- `MaxMemoryBytes` is a caller-supplied logical reservation budget. It is not
  a process heap measurement. A request larger than the class budget fails;
  requests that would exceed currently reserved bytes wait in the normal
  bounded queue.
- Zero class limits preserve the existing behavior. Classes without a
  positive memory budget do not reserve or account memory.
- Negative class limits and negative request reservations are rejected.
- `ClassStats` exposes active, pending, configured concurrency, and reserved
  memory for monitoring.

## Usage

`Acquire` and `Run` remain source-compatible and use the original fast path
when no class budget is configured. For a memory-aware operation, use
`RunWithMemory` in the hot path:

```go
err = admission.RunWithMemory(ctx, "analytics", 32<<20, func(ctx context.Context) error {
    return executeQuery(ctx)
})
```

`AcquireWithMemory` is available when the caller needs an explicit lease. Its
returned release function has a small closure cost; `RunWithMemory` avoids that
allocation.

## Measurements

Linux/amd64, AMD Ryzen 9 5950X, five samples per case. The pre-change values
are the legacy controller before adding class budgets; post-change values are
from the final implementation. The default path remains allocation-free.

| Case | Before median | After median | Memory after | Relative result |
| --- | ---: | ---: | ---: | ---: |
| Legacy `Acquire` | 25.47 ns/op | 23.96 ns/op | 0 B/op, 0 allocs/op | 1.06x faster |
| Legacy `Run` | 51.41 ns/op | 50.86 ns/op | 0 B/op, 0 allocs/op | 1.01x faster |
| `AcquireWithMemory` | not available | 112.2 ns/op | 64 B/op, 2 allocs/op | opt-in lease path |
| `RunWithMemory` | not available | 55.61 ns/op | 0 B/op, 0 allocs/op | preferred opt-in path |

Raw final samples are recorded in [BENCHMARK.md](BENCHMARK.md#c231-sql-workload-groups).
