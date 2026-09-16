# MZ-40 Recursive Convergence Bounds

The mutable recursive reachability maintainer now has an opt-in bounded apply
API:

```go
updates, stats, err := reachability.ApplyWithOptions(
	[]hatSql.RecursiveReachabilityMutation{{
		Kind: hatSql.RecursiveReachabilityInsert,
		Key:  "de",
		From: "d",
		To:   "e",
	}},
	hatSql.RecursiveReachabilityApplyOptions{
		MaxAffectedSources: 10000,
		MaxIterations:      128,
		MaxTraversalSteps:  100000,
		MaxEmittedUpdates:  10000,
	},
)
```

`MaxAffectedSources` limits source arrangements that must be recomputed.
`MaxIterations` limits the deepest recursive path visited. `MaxTraversalSteps`
limits total reverse-source and forward-closure visits. `MaxEmittedUpdates`
limits the signed differential rows produced by the batch. Zero leaves an
individual limit unlimited; negative values are rejected.

The plan is built on temporary maps and every limit is checked before the
maintainer publishes the new edge, closure, ancestor index, or differential
rows. A rejected batch returns an error matching
`ErrIncrementalRecursiveReachabilityLimitExceeded` and leaves the graph
unchanged. `RecursiveReachabilityApplyStats` reports observed work on both
success and rejection, which lets a caller tune limits from production data.

The existing `Apply` method is unchanged and remains the default hot path. A
single insert using only affected-source and emitted-update bounds reuses the
existing incremental append algorithm after an exact preflight count, so it
does not walk the whole graph. Full depth or traversal-step bounds use the
general bounded planner and intentionally cost more because they collect the
requested safety telemetry.

`ApplyWithOptions` is available for mutable maintainers. Append-only
maintainers should continue to use `Append`; calling the bounded mutable API
on one returns `ErrIncrementalRecursiveReachabilityMutationsDisabled`.

## Benchmark

Machine: AMD Ryzen 9 5950X, Linux/amd64. Each row is the median of three
samples from `make benchmark-mz040-before-c203` or
`make benchmark-mz040-c203`, with `-benchtime=20ms`.

The operation uses a 255-edge chain and appends one tail edge. The benchmark
timer excludes rebuilding the seeded maintainer, but reports allocations made
by the timed operation.

| Path | ns/op | B/op | allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing `Apply` control | 30,029,433 | 26,122,280 | 171,002 | 1.00x |
| `ApplyWithOptions` source/update bounds | 28,968,193 | 26,162,632 | 171,003 | 1.04x faster |
| `ApplyWithOptions` full bounds | 41,189,034 | 34,991,008 | 177,715 | 1.37x slower |

The source/update-bounds path is effectively allocation-neutral in this
workload. Full convergence accounting is a deliberate operational tradeoff,
not a default performance optimization; use it when preventing runaway
recursive work matters more than the extra planning cost.

### Raw Results

```text
# clean baseline control: make benchmark-mz040-before-c203
full_recompute:             166305, 154948, 161915 ns/op; 114832 B/op; 1031 allocs/op
incremental_append:           1563,   1573,   1528 ns/op;   1088/1063/1072 B/op; 14 allocs/op
full_recompute_delete:      159851, 155911, 160517 ns/op; 114752 B/op; 1028 allocs/op
full_recompute_update:      156521, 161691, 155437 ns/op; 114768 B/op; 1029 allocs/op
mutable_delete:              13458,  14311,  13924 ns/op;    496 B/op; 7 allocs/op
mutable_update:              18782,  19938,  21177 ns/op;   1368 B/op; 18 allocs/op

# feature run: make benchmark-mz040-c203
legacy_apply:            30775792, 30029433, 26793534 ns/op; 26127760/26122280/26116624 B/op; 171009/171002/170996 allocs/op
bounded_apply:            29439426, 26058625, 28968193 ns/op; 26168080/26162632/26156944 B/op; 171008/171003/170995 allocs/op
fully_bounded_apply:      41189034, 41442861, 39674403 ns/op; 34991328/34990992/34991008 B/op; 177717/177714/177715 allocs/op
```

The legacy control subbenchmarks also ran after the feature in
`make benchmark-mz040-c203`; the implementation does not modify `Apply`, and
the small differences between separate benchmark processes are treated as
noise rather than claimed as improvements.
