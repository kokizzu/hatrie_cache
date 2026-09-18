# TR-31: Automatic Index Choice

`hatSql` automatically chooses among eligible equality indexes for an `AND`
predicate when the resolver exposes cardinality statistics or exact value
estimates. It tries the smallest estimated posting first, retries another
candidate when an index is unavailable, and always evaluates the complete
predicate after candidate retrieval.

The execution result exposes the decision through an `INDEX CANDIDATES`
`ExplainStep`. Each alternative includes its expression, estimated rows,
estimated probe cost, and either `selected` or a rejection reason such as
`index unavailable`. Adaptive feedback can additionally report
`adaptive scan selected after prior underestimate`.

```go
planner := hatSql.NewAdaptivePlanner(hatSql.AdaptivePlannerOptions{
    MinSamples:          3,
    UnderestimateFactor: 4,
})
result, err := hatSql.ExecuteSQLQueryParameters(ctx, query, resolver, nil,
    hatSql.SQLQueryOptions{AdaptivePlanner: planner})
```

`AdaptivePlanner` is opt-in. A nil planner preserves the default path and its
zero feedback overhead. The planner is only a work-selection hint; it cannot
change query results because the residual predicate is still evaluated.

## Scope

- Equality conjunctions can use per-value exact estimates or average index
  statistics.
- Candidates are ordered by estimated posting cardinality, with stable
  left-to-right ties.
- Missing, unsupported, or unavailable indexes fall back to the established
  source scan and are visible in `EXPLAIN ANALYZE`.
- `FORCE` and `FORBID` index hints remain explicit diagnostic overrides.
- Adaptive feedback is intended for callers that can provide reliable source
  and index costs. It is not enabled by default.

## Benchmark

Command:

```text
make benchmark-tr31
```

The fixture has 20,000 rows, a 50%-selective `kind` predicate, and a
single-row `id` predicate. Five samples on Linux/amd64 with an AMD Ryzen 9
5950X produced these medians:

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Historical left-to-right probe | 2,906,745 | 2,589,384 | 66 | Baseline |
| Estimated-selectivity choice | 586,590 | 10,739 | 63 | `4.95x` faster; `241x` lower bytes; `4.5%` fewer allocations |

The adaptive feedback sub-benchmark deliberately lies about a 20,000-row
posting and compares the bad index with the fallback source scan. The current
working-tree samples were `43.46 ms/op`, `24.68 MB/op`, and `460,080`
allocations for the feedback fallback versus `4.43 ms/op`, `6.43 MB/op`, and
`20,027` allocations for the bad-index control. That is a resolver-specific
stress case, not a default-path claim; it is why adaptive feedback remains
opt-in and should be measured with the caller's resolver.

The adaptive regression fix was tested against the previous commit. In that
same stress fixture, the old code incorrectly kept probing the bad index at a
median `28.72 ms/op`, `14.44 MB/op`, and `300,062` allocations. The new code
correctly switches to the scan, at a median `43.46 ms/op`, `24.68 MB/op`, and
`460,080` allocations. The correctness fix is retained because the old path
violated its documented adaptive behavior; no default execution path changed.

Focused verification:

```text
make test-tr31-focused
```

This covers candidate selection, unavailable-index retry, explain output,
single-predicate adaptive feedback, multi-conjunct adaptive feedback, race
detection, and vet.
