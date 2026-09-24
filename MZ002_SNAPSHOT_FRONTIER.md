# MZ-002 Snapshot Frontier Gating

## Idea

Materialize-style dataflow readers need a common readiness boundary before
reading a consistent view across several independently advancing sources.
`hatPipeline.FrontierRegistry` already exposes monotone named lower/upper
frontiers, but callers previously had to coordinate one `WaitUntil` call and
one snapshot copy per source themselves.

## Adopted API

`hatPipeline.SnapshotFrontierGate` is an opt-in, immutable coordinator over a
fixed set of already registered frontier IDs:

```go
registry, _ := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
_ = registry.Register("orders")
_ = registry.Register("payments")

gate, err := hatPipeline.NewSnapshotFrontierGate(
	hatPipeline.SnapshotFrontierGateOptions{
		Registry:  registry,
		SourceIDs: []string{"orders", "payments"},
	},
)
if err != nil {
	panic(err)
}

snapshot, err := gate.WaitUntil(ctx, 100)
if err != nil {
	return err
}
// Every snapshot.Sources entry has Lower >= 100.
_ = snapshot
```

The constructor rejects nil registries, empty or duplicate IDs, and IDs that
are not registered. IDs are copied and sorted once. `Ready` is a non-blocking
check, `Snapshot` returns an isolated ready snapshot, and `WaitUntil` blocks
until every source reaches the target or the context/registry ends.

The registry's monotonic lower frontier makes the returned boundary stable:
once every source has `Lower >= target`, later source progress cannot invalidate
that target. The gate does not start workers, retain goroutines, or alter the
existing `Advance` hot path.

## Benchmark

Linux amd64, AMD Ryzen 9 5950X, five samples per benchmark, `-benchmem`.
The manual path is the pre-feature caller pattern: wait on each source and
copy each `FrontierSnapshot`. The gate path uses `WaitUntil` and returns the
same number of snapshots. Values below are medians from the same comparison
run; `manual/gate` is the relative speed, so values below `1x` mean the opt-in
gate is slower.

| Sources | Manual ns/op | Gate ns/op | Manual/Gate | Manual B/op | Gate B/op | Manual allocs/op | Gate allocs/op |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 77.73 | 78.20 | 0.99x | 64 | 64 | 1 | 1 |
| 8 | 502.8 | 535.5 | 0.94x | 512 | 512 | 1 | 1 |
| 64 | 4,813 | 4,678 | 1.03x | 4,864 | 4,864 | 1 | 1 |

The gate has no default-path cost because it is opt-in, and it preserves the
manual allocation and memory profile. Its small throughput variation comes
from the public method boundary; the tradeoff buys a reusable correctness
boundary and consistent source ordering. Constructor validation is one-time:

| Sources | Constructor ns/op | B/op | allocs/op |
| ---: | ---: | ---: | ---: |
| 1 | 100.3 | 48 | 2 |
| 8 | 489.2 | 160 | 2 |
| 64 | 7,287 | 4,680 | 5 |

Reuse a gate for repeated reads so this setup cost is amortized.

## Limits

- The gate does not read or transactionally freeze source data.
- Source progress and source registration remain caller-owned.
- A source removed after construction causes a subsequent wait/snapshot to
  fail rather than silently switching to a newly registered source with the
  same ID.
- Durable snapshot persistence and cross-process coordination remain open
  work in MZ-001/MZ-003.
