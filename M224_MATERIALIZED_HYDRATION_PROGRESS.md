# M224 Materialized Hydration Progress

M224 adds observable progress and bounded work estimates to the M223
materialized-view hydration state machine. It is inspired by Materialize's
operational visibility for long-running dataflow work.

## Public API

`MaterializedViewStatus.HydrationProgress` contains:

- `CompletedWork`: source-row work observed so far.
- `EstimatedWork`: the sum of source cardinality estimates for the declared
  dependencies. It is a work estimate, not the final result row count.
- `EstimatedRemainingWork`: the non-negative difference while the estimate is
  available; it is zero when no trustworthy estimate exists.
- `Progress`: a fraction from `0` to `1`. It is `1` after hydration succeeds,
  even when the source did not provide an estimate.
- `ProgressKnown`: whether a fraction can be computed from an estimate.
- `EstimateAvailable`: whether every declared dependency supplied a valid
  cardinality.
- `EstimateExact`: whether every supplied cardinality was exact.
- `StartedAt`: UTC start time for the current hydration attempt.

Existing `SourceResolver` implementations remain valid. Implement
`SourceCardinalityResolver` to provide the estimate and
`HydrationProgressSourceResolver` to report live source-row progress:

```go
func (resolver myResolver) SQLSourceCardinality(name, key string) (int, bool, bool, error) {
	return resolver.rowCount(name, key), true, true, nil
}

func (resolver myResolver) ResolveSQLSourceWithProgress(
	ctx context.Context,
	name, key string,
	report func(hatSql.Row) error,
) ([]hatSql.Row, error) {
	rows, err := resolver.load(ctx, name, key)
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if err := report(row); err != nil {
			return nil, err
		}
	}
	return rows, nil
}
```

The progress callback is accounting-only and must not retain its row. Returned
rows may be retained by the current query, so the resolver must not mutate
them concurrently until that query completes. This ownership rule avoids a
second full row copy in the opt-in path.

When a resolver does not implement the optional interfaces, hydration still
works. The status reports an explicit unknown estimate instead of fabricating
a remaining-work value. Invalid, negative, unavailable, or errored cardinality
is ignored for progress estimation and never makes an otherwise valid hydrate
fail.

## Lifecycle

`CreateCold` starts with zero progress. `Hydrate` publishes `hydrating` with
the estimate before source work begins. `Get` can then be polled without
reading query rows or retaining row payloads. A successful hydrate publishes
`ready`, sets remaining work to zero, and records a complete fraction. A
source, cancellation, storage-budget, or point-lookup-build failure returns
the view to `cold` and clears the attempt's progress. Legacy synchronous
`Create` remains `ready` and retains its existing behavior.

## Measurement

Command:

```text
make benchmark-m224-materialized-hydration-progress
```

The benchmark hydrates 256 rows, runs five samples per case, and reports
`-benchmem` results. Medians from the measured run:

| Case | ns/op | B/op | allocs/op | Feature / control |
| --- | ---: | ---: | ---: | ---: |
| Legacy `SourceResolver` control | 274,309 | 271,052 | 1,569 | 1.00x |
| Opt-in progress + cardinality resolver | 233,477 | 232,332 | 1,067 | 0.85x / 0.86x / 0.68x |

The opt-in path is faster and smaller in this workload because its ownership
contract avoids the legacy source-row copy while adding the atomic progress
counter. The comparison is intentionally labeled: it compares the public
legacy resolver contract with the new opt-in contract, not two identical
resolver implementations.

Raw samples:

```text
Progress ns/op: 236696 234598 225648 230089 233477
Progress B/op:  232335 232332 232331 232331 232331
Progress allocs: 1067 1067 1067 1067 1067
Control ns/op: 274309 278936 277887 268453 253151
Control B/op:  271053 271052 271053 271052 271052
Control allocs: 1569 1569 1569 1569 1569
```

## Verification

- `make test-m224-materialized-hydration-progress`
- `make test-m223-related-materialized`
- `make race-m224-materialized-hydration-progress`
- `make vet-m224-materialized-hydration-progress`

The full `hat/hatSql` package run still has pre-existing typed-table
arrangement checkpoint failures recorded before M224; no M224 test failure was
observed.
