# M218 Point-Lookup Planner Selection

M218 adds `hatSql.PlanSQLPointLookup`, a deterministic advisory planner for
choosing a maintained point-lookup arrangement or a source scan. It builds on
the existing `SQLArrangementCostModel` and is intended to consume current
source/index estimates supplied by a caller. It does not create, hydrate, or
drop arrangements, and it does not change the default SQL executor path.

## Example

```go
plan := hatSql.PlanSQLPointLookup([]hatSql.SQLPointLookupCandidate{
	{
		SQLArrangementCostCandidate: hatSql.SQLArrangementCostCandidate{
			Key:              "accounts_by_region",
			Field:            "region",
			Kind:             "point-lookup",
			BuildCostNanos:   100,
			ProbeCostNanos:   100_000,
			ScanCostNanos:    300_000,
			MaintenanceNanos: 100,
			ExpectedReads:    100,
			ExpectedWrites:   1,
			MemoryBytes:      8 << 20,
		},
		Available: true,
	},
}, hatSql.SQLPointLookupPlanOptions{MemoryBudgetBytes: 16 << 20})

if plan.Strategy == hatSql.SQLPointLookupArrangement {
	rows := maintainedIndex.Lookup("ap-southeast")
	_ = rows
} else {
	// Execute the ordinary source scan.
}
```

## Selection Rules

An arrangement is eligible only when it is marked `Available`, fits the
configured memory budget, has positive modeled probe savings over the scan,
and its build plus expected maintenance cost repays within `ExpectedReads`.
`MinimumNetBenefitNanos` can require additional modeled benefit. Candidates
are ranked by eligibility, net benefit, payback reads, memory, and stable
key/field/kind identity. The input slice is never modified.

When no candidate is eligible, `Strategy` is `scan` and `Reason` explains the
best rejected candidate: unavailable, memory budget, no read savings,
insufficient reads, or insufficient benefit. The returned candidate scores are
bounded, deterministic EXPLAIN material and retain no query text, literal, or
row data.

The planner is deliberately caller-driven. Automatic SQL integration must
still account for source freshness, arrangement hydration, transaction
visibility, and query-specific selectivity; those concerns remain outside this
small cost-selection API.

## Measurement

Linux `amd64`, AMD Ryzen 9 5950X, `go test -benchmem -count=5 -benchtime=200ms`.
The fixture has 10,000 complete rows and 156 matching rows for `team-42`. The
baseline always scans. The selected path includes one planner decision and the
maintained M217 point lookup in the timed loop.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative |
| --- | ---: | ---: | ---: | --- |
| Always scan | 272,724 | 53,703 | 313 | 1.00x |
| Cost-based plan plus point lookup | 94,425 | 61,944 | 316 | 2.89x faster; 15.3% more B/op; +3 allocs |
| Planner only, 32 candidates | 7,431 | 9,832 | 4 | Planning overhead, not row execution |

The arrangement path remains opt-in because it retains maintained rows and
the planner requires trustworthy estimates. A caller with no current index or
source statistics receives the ordinary scan decision.

Raw benchmark samples:

```text
BenchmarkM218BaselineAlwaysScanLookup-32       766 294908 ns/op 156.0 result_rows 53697 B/op 313 allocs/op
BenchmarkM218BaselineAlwaysScanLookup-32      1010 270181 ns/op 156.0 result_rows 53708 B/op 313 allocs/op
BenchmarkM218BaselineAlwaysScanLookup-32       901 265147 ns/op 156.0 result_rows 53703 B/op 313 allocs/op
BenchmarkM218BaselineAlwaysScanLookup-32       835 294703 ns/op 156.0 result_rows 53697 B/op 313 allocs/op
BenchmarkM218BaselineAlwaysScanLookup-32       830 272724 ns/op 156.0 result_rows 53704 B/op 313 allocs/op
BenchmarkM218CostBasedPointLookup-32          2643  91211 ns/op 156.0 result_rows 61948 B/op 316 allocs/op
BenchmarkM218CostBasedPointLookup-32          2391  96708 ns/op 156.0 result_rows 61944 B/op 316 allocs/op
BenchmarkM218CostBasedPointLookup-32          2343  98335 ns/op 156.0 result_rows 61944 B/op 316 allocs/op
BenchmarkM218CostBasedPointLookup-32          2331  92478 ns/op 156.0 result_rows 61946 B/op 316 allocs/op
BenchmarkM218CostBasedPointLookup-32          2552  94425 ns/op 156.0 result_rows 61944 B/op 316 allocs/op
BenchmarkM218PlannerDecision32Candidates-32  34004   6601 ns/op                     9832 B/op 4 allocs/op
BenchmarkM218PlannerDecision32Candidates-32  34242   7514 ns/op                     9832 B/op 4 allocs/op
BenchmarkM218PlannerDecision32Candidates-32  31214   7431 ns/op                     9832 B/op 4 allocs/op
BenchmarkM218PlannerDecision32Candidates-32  30348   7581 ns/op                     9832 B/op 4 allocs/op
BenchmarkM218PlannerDecision32Candidates-32  33954   6967 ns/op                     9832 B/op 4 allocs/op
```

## Verification

The tests cover arrangement selection, unavailable and over-budget fallback,
no-savings fallback, payback gating, deterministic ties, and input
immutability. Verification runs:

```text
make test-m218-red
make test-m218-green
make benchmark-m218
make verify-m218
```
