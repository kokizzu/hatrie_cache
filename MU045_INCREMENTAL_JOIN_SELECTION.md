# M-U45 Incremental Join Index Selection

`hatSql.SQLIncrementalJoinSelector` is an opt-in, bounded catalog for
choosing among caller-maintained incremental join arrangements when query
predicates change. It indexes candidates by normalized
`left_source/right_source/left_field/right_field`, so a request does not scan
unrelated join definitions.

The selector is deliberately advisory. It does not create, drop, hydrate, or
mutate a join. The caller remains responsible for constructing the selected
arrangement, applying source changes, and treating `refresh` as a required
catch-up step before reading stale state.

## Example

```go
selector := hatSql.NewSQLIncrementalJoinSelector(hatSql.SQLIncrementalJoinSelectorOptions{
	Capacity:   128,
	MinSamples: 2,
})

selector.ObserveJoin(hatSql.SQLIncrementalJoinCandidate{
	Key:              "orders_by_user",
	LeftSource:       "orders",
	RightSource:      "users",
	LeftField:        "user_id",
	RightField:       "id",
	SourceGeneration: 42,
	ScanCostNanos:    100,
	ProbeCostNanos:   4,
	BuildCostNanos:   1000,
	MaintenanceNanos: 2,
	ExpectedReads:    100,
	ExpectedWrites:   10,
	MemoryBytes:      4096,
})

decision := selector.SelectJoin(hatSql.SQLIncrementalJoinRequest{
	LeftSource:       "orders",
	RightSource:      "users",
	LeftField:        "user_id",
	RightField:       "id",
	SourceGeneration: 42,
})
switch decision.Action {
case hatSql.SQLIncrementalJoinReuse:
	// Read the selected arrangement.
case hatSql.SQLIncrementalJoinRefresh:
	// Apply source changes through decision.Candidate.SourceGeneration.
case hatSql.SQLIncrementalJoinCreate:
	// Build a new arrangement and record later observations.
}
```

`SourceGeneration` is a monotone checkpoint. A candidate ahead of the request
is ignored; a candidate behind it returns `refresh`; an equal generation can
return `reuse`. The default minimum sample count is `2`, and the default
catalog capacity is `128` candidates, capped at `1024`. Invalid or overlong
identity components are rejected rather than truncated.

Repeated observations retain the largest observed cost and memory values and
accumulate expected reads and writes. This makes selection conservative under
changing workload measurements. `Snapshot` is deterministic and independent,
and `Reset` explicitly starts a new workload window.

## Verification and measurement

```sh
make format-mu045-incremental-join-selection
make test-mu045-incremental-join-selection
make race-mu045-incremental-join-selection
make vet-mu045-incremental-join-selection
make benchmark-mu045-incremental-join-selection
```

The benchmark compares indexed selection with a linear scan over the same 128
candidate catalog. It measures planning/catalog lookup only; it is not a claim
about the cost of executing the join itself. See the M-U45 section in
`BENCHMARK.md` for the raw samples and tradeoff.
