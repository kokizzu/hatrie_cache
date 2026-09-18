# MZ-036 Dataflow Operator Placement

`hatPipeline.PlanDataflowOperatorPlacement` is an opt-in, deterministic
control-plane planner for assigning operator replicas to workers. It is
separate from [FAILURE_DOMAIN_PLACEMENT.md](FAILURE_DOMAIN_PLACEMENT.md),
which validates shard replica admission during cluster changes.

The planner supports:

- worker capacity shared across all operators;
- a minimum number of distinct failure domains for each operator;
- an optional allow-list of failure domains per operator;
- one worker per operator replica, deterministic operator and worker ordering;
- bounded input and output sizes with sane defaults.

Zero values use these defaults:

| Setting | Default |
| --- | ---: |
| `Replicas` | `1` |
| `MinFailureDomains` | `1` |
| worker `Capacity` | `1` |
| `MaxOperators` | `4096` |
| `MaxWorkers` | `256` |
| `MaxAssignments` | `1048576` |

The planner first spreads replicas across unused domains until the requested
minimum is met, then chooses the least-loaded eligible worker. It returns an
error rather than silently violating a constraint. Input slices are not
modified. A plan is metadata only: it does not start, stop, migrate, or
rebalance running operators.

Example:

```go
plan, err := hatPipeline.PlanDataflowOperatorPlacement(
    []hatPipeline.DataflowPlacementOperator{
        {ID: "join", Replicas: 3, MinFailureDomains: 3},
    },
    []hatPipeline.DataflowPlacementWorker{
        {ID: "worker-a", FailureDomain: "zone-a", Capacity: 2},
        {ID: "worker-b", FailureDomain: "zone-b", Capacity: 2},
        {ID: "worker-c", FailureDomain: "zone-c", Capacity: 2},
    },
    hatPipeline.DataflowPlacementOptions{},
)
```

The caller applies `plan.Assignments` to its dataflow runtime and owns the
lifecycle of any movement. This keeps existing pipelines unchanged and avoids
turning placement calculation into an implicit rebalance operation.

## Measured Cost

The benchmark uses 32 operators, three replicas each, and 16 workers. The
round-robin baseline only assigns slots and does not enforce capacity or
failure-domain constraints, so it is not a correctness-equivalent
replacement. Results are five 500 ms runs on Linux amd64, AMD Ryzen 9 5950X:

| Operation | Median ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| unchecked round-robin baseline | 532.6 | 0 | 0 | 1.0x |
| constrained placement solver | 57,193 | 12,512 | 15 | 107.4x |

This is a planning-time cost, not a per-row cost. Callers should calculate a
plan when a dataflow is created or intentionally rescheduled, retain it, and
avoid recalculating it for individual records. Raw output is recorded in
[BENCHMARK.md](BENCHMARK.md#mz-036-dataflow-operator-placement).
