# Automatic Dataflow Index Advisor

`hat/hatSql` provides an opt-in, in-memory advisor for applications that want
to turn observed dataflow activity into bounded index recommendations. It is
inspired by Materialize's practice of using observed dataflows to guide
arrangement reuse, while keeping index creation and plan changes under the
application's control.

## Usage

Create an advisor and feed it observations from the application's query or
dataflow metrics path:

```go
advisor := hatSql.NewSQLDataflowIndexAdvisor(hatSql.SQLDataflowIndexAdvisorOptions{
	Capacity:   128,
	MinSamples: 2,
	CostModel: hatSql.SQLArrangementCostModelOptions{
		MemoryBudgetBytes: 64 << 20,
	},
})

advisor.ObserveDataflows([]hatSql.SQLDataflowIndexObservation{
	{
		Candidate: hatSql.SQLArrangementCostCandidate{
			Key:              "users",
			Field:            "region",
			BuildCostNanos:   1000,
			ProbeCostNanos:   20,
			ScanCostNanos:    500,
			MaintenanceNanos: 5,
			ExpectedReads:    100,
			ExpectedWrites:   1,
			MemoryBytes:      4096,
		},
	},
})

for _, recommendation := range advisor.Recommendations(10) {
	fmt.Printf("%s.%s payback=%d benefit=%d ns\n",
		recommendation.Candidate.Key,
		recommendation.Candidate.Field,
		recommendation.PaybackReads,
		recommendation.NetBenefitNanos)
}
```

`ObserveDataflow` is useful for one event. `ObserveDataflows` accepts a batch
and takes one lock for the complete batch, which is the preferred path for
metrics flushes. The advisor is safe for concurrent observation and reads.

## Behavior

- The default capacity is 128 distinct candidates.
- The default minimum is two samples per candidate. A candidate with fewer
  samples is not recommended.
- Candidate identity is `(Key, Field, Kind)`. Empty keys or fields are
  rejected; repeated observations of an existing identity remain accepted
  even when the capacity limit has been reached.
- Sample counts and expected read/write totals use saturating arithmetic.
  Build, probe, scan, maintenance, and memory estimates retain the largest
  value observed for that candidate.
- Recommendations are evaluated by `SQLArrangementCostModel`, so the memory
  budget and payback rules are shared with the MZ032 cost model. Only
  candidates marked reusable by that model are returned.
- Results are sorted by net benefit, payback, and candidate identity, making
  output deterministic. A positive `limit` truncates the result; zero returns
  all recommendations.
- `Reset` clears the observation window while retaining the advisor's map
  allocation and configuration.

This is advisory metadata only. The advisor does not create indexes, alter
`SQLQueryOptions`, retain SQL text, retain row values, or start a background
worker. Applications decide how to validate, build, persist, and retire an
index. Callers handling untrusted input should bound candidate key and field
lengths before observing them.

## Resource Cost

Observation is amortized O(1) per event and a batch is O(n). Recommendation
generation is O(k log k) for `k` retained candidates. Memory is bounded by the
configured candidate capacity plus the caller's observation slice; the advisor
does not retain the input slice after `ObserveDataflows` returns.

The benchmark uses 10,000 observations over 512 candidate identities, reuses
one advisor per benchmark process, resets between workload windows, and feeds
the observations through the batch API. See the MZ033 section in
`BENCHMARK.md` for the raw samples and comparison with a manual aggregation
implementation.

The implementation and executable example live in `hat/hatSql`; run:

```text
make test-mz033
make benchmark-mz033
```
