# M215 Delta-Join Maintenance

M215 adds an opt-in differential output path for the existing incremental typed
table join. Use `ApplyLeftDeltas` or `ApplyRightDeltas` when a downstream
consumer can apply join-result changes instead of requesting the complete
materialized result after every high-churn input update.

```go
deltas, err := join.ApplyLeftDeltas(changes)
if err != nil {
	// A batch may have applied a valid prefix; inspect deltas before retrying.
	return err
}
for _, delta := range deltas {
	// Diff is +1 for an inserted result and -1 for a retracted result.
	consume(delta.LeftKey, delta.RightKey, delta.Diff, delta.Left, delta.Right)
}
```

`TypedTableJoinArrangement` exposes the same methods for shared join leases.
The existing `ApplyLeft`, `ApplyRight`, and `Rows` APIs are unchanged.

## Semantics

- Deltas contain independent copies of both source rows.
- Updates that change a matching row emit a retraction for the old pair and an
  insertion for the new pair.
- Consecutive changes for one source key use the join's existing batch
  coalescing behavior, so transient intermediate pairs are not emitted.
- Output is deterministic by left key, right key, then diff.
- A stale/replayed change that does not alter the join emits no delta.
- If a gap or invalid change occurs after a valid prefix, the method returns
  the deltas for the state transition that actually applied and the error.
- Semijoin-reduced joins use the same pending-row activation and demotion rules
  as the existing full-state path.

The implementation captures only pairs involving changed source keys, applies
the existing locked join maintenance, and diffs the affected state. It does
not remove the maintained pair index or reduce the memory needed by `Rows()`;
that is deliberate so existing behavior and default performance remain stable.

## When To Use It

Use the delta API for projections, caches, or sinks that already maintain their
own keyed state. Keep using `Rows()` when the caller needs a complete ordered
snapshot. `ApplyLeftDeltas` has output construction cost and can be slower than
`ApplyLeft` alone for callers that discard the output; it is a replacement for
`Apply` plus full `Rows()`, not for the state-only operation.

## Verification

Focused tests cover left and right updates, old/new row ownership, consecutive
same-key batch coalescing, shared-arrangement access, and unchanged maintained
rows. The package test, race test, and vet test pass:

```text
make m215-test
make m215-race
make m215-vet
```

## Benchmark

Five `-benchmem` samples were run on Linux/amd64 with an AMD Ryzen 9 5950X.
The fixture has 256 left and 256 right rows on one hot join key, so each update
touches 256 result pairs.

| Path | Median ns/op | B/op | Allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Existing `ApplyLeft` only | 22,832 | 288 | 3 | 1.00x |
| Existing `ApplyLeft` plus full `Rows()` | 30,991,494 | 17,826,412 | 131,085 | 1,357x |
| `ApplyLeftDeltas` | 382,784 | 408,506 | 1,061 | 16.8x vs apply-only |

Compared with the full snapshot path, differential output is about `81x`
faster, uses `44x` fewer allocated bytes, and uses `124x` fewer allocations.
Compared with `ApplyLeft` alone, it costs about `16.8x` CPU and `1,419x`
allocated bytes because it must materialize the 512 signed pair deltas. That
is the measured cost of producing useful output and is avoided when callers
continue using the state-only API.

Raw samples:

```text
Rows ns/op:       31693152 30991494 29743320 32470728 29892833
Rows B/op:        17826544 17826541 17826383 17826412 17826382
Rows allocs/op:     131085   131085   131085   131085   131085
Apply ns/op:          22811    22832    23382    23169    22719
Apply B/op:             288      288      288      288      288
Apply allocs/op:          3        3        3        3        3
Deltas ns/op:         375259   397721   382784   375871   390761
Deltas B/op:          408506   408506   408505   408506   408506
Deltas allocs/op:       1061     1061     1061     1061     1061
```
