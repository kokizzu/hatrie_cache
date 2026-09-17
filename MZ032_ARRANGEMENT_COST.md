# MZ-032 Arrangement Cost Model

This adds a small, importable `hatSql` cost model inspired by Materialize's
reuse of arrangements across dataflow operators. It scores an existing or
planned index, projection, or ordered layout using the caller's workload
estimates:

- read savings: `max(scan cost - probe cost, 0)`;
- total cost: `build cost + maintenance cost * expected writes`;
- expected benefit: `read savings * expected reads`;
- net benefit: `expected benefit - total cost`;
- payback reads: `ceil(total cost / read savings)`.

All arithmetic saturates instead of wrapping. Costs are named in nanoseconds
because that is the usual input, but the model only compares the units
provided by the caller.

## Usage

The model is opt-in and does not create, drop, or modify an arrangement:

```go
model := hatSql.NewSQLArrangementCostModel(
    hatSql.SQLArrangementCostModelOptions{MemoryBudgetBytes: 1024},
)
ranked := model.Rank([]hatSql.SQLArrangementCostCandidate{{
    Key:              "users",
    Field:            "region",
    BuildCostNanos:   100,
    ProbeCostNanos:   10,
    ScanCostNanos:    110,
    MaintenanceNanos: 5,
    ExpectedReads:    20,
    ExpectedWrites:   2,
    MemoryBytes:      512,
}})
```

The candidate is reusable because its 100-unit read saving pays back its
110-unit build and write-maintenance cost in two reads. A caller can use
`score.Reusable` before sharing or creating the layout, and can inspect
`NetBenefitNanos`, `PaybackReads`, `FitsMemory`, and the other returned
diagnostics.

`Evaluate` is O(1) and does not allocate. `Rank` returns scores ordered by
reusable status, net benefit, payback, memory, and candidate identity.
`RankInto` lets a caller reuse the destination backing array; ranking itself is
O(n log n).

## Defaults And Boundaries

- A zero memory budget means no memory limit.
- A zero minimum net benefit becomes a one-unit positive threshold, so
  break-even candidates are rejected.
- A candidate with no positive read savings is never reusable.
- A zero total cost and positive read savings means immediate payback
  (`PaybackReads == 0`) and can be reusable.
- Candidates are estimates supplied by the caller; the model does not collect
  query telemetry or silently change SQL planning.
- Existing SQL execution and storage defaults are unchanged. No
  `SQLQueryOptions` field is enabled by this feature.

This makes the feature safe for applications that already have index,
projection, or workload statistics while leaving policy and lifecycle control
with the application.

## Measurement

Command: `make benchmark-mz032`

The benchmark evaluates 10,000 prebuilt candidates per operation, runs 1,000
operations per sample, and takes five samples for each path on Linux amd64
with `GOMAXPROCS=1` and an AMD Ryzen 9 5950X 16-Core Processor. Both paths
compute the same full score, and
`TestSQLArrangementCostModelMatchesBenchmarkFormula` verifies every fixture
result.

| Path | Raw ns/op samples | Median ns/op | B/op | Allocs/op | Improvement |
| --- | --- | ---: | ---: | ---: | --- |
| Independent manual scorer | 245,775; 246,117; 241,523; 237,556; 235,561 | 241,523 | 0 | 0 | baseline |
| `SQLArrangementCostModel.Evaluate` | 222,526; 221,757; 221,003; 222,149; 223,347 | 222,149 | 0 | 0 | 1.09x CPU, 8.0% lower ns/op |

The improvement is for the complete score calculation, not for SQL execution:
the default query path never calls this opt-in model. `Rank` additionally
materializes and sorts score results; use `RankInto` when retaining a reusable
destination is important.
