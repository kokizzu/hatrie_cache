# M224: Hydration Progress And Estimated Remaining Work

M224 extends `MaterializedViews.HydrationStatus` with live rebuild progress for
optional point postings. It reuses the bounded rebuild queue's existing
monotonic completed/total counters and does not add a per-row progress map.

## Status Fields

`MaterializedViewHydrationStatus` now reports:

| Field | Meaning |
| --- | --- |
| `Completed` | Completed row ordinal reported by the rebuild callback. |
| `Total` | Total rows in the captured snapshot. |
| `Progress` | Fraction from `0` through `1`; `0` means no known work yet. |
| `EstimatedRemainingSeconds` | Wall-clock estimate while running and after at least one completed batch; zero when unavailable or finished. |

Queued work reports `Completed: 0`, `Total: 0`, and `Progress: 0` until the
worker starts and publishes its first progress callback. A completed empty
build reports `Progress: 1`. Failed or canceled replacement builds retain the
currently published index state and expose the last queue counters.

The estimate is deliberately conservative and local to the current worker:

```text
elapsed_seconds * (total - completed) / completed
```

It is an estimate, not a deadline or service-level guarantee. It is omitted
from JSON when zero. A caller can also set the optional `Progress` callback on
`MaterializedViewPointLookupBuildRequest` to receive the same monotonic
callbacks without polling.

For replicated builds, the logical hydration state still follows the configured
quorum. Once quorum succeeds, progress is taken from a successful replica;
while quorum is pending, the most advanced active replica is used for the
displayed frontier. Each replica continues to retain its own queue status.

## Cost

The feature adds arithmetic and, for a running task, one `time.Since` call to
the status path. It does not change `PointLookup` or the retained posting
layout. Same-host `-benchmem` results:

| Status path | Median ns/op | B/op | allocs/op | Relative to M223 baseline |
| --- | ---: | ---: | ---: | ---: |
| M223 ready status | 93.00 | 16 | 1 | 1.00x |
| M224 ready status | 146.8 | 16 | 1 | 1.58x slower |
| M224 running status | 159.0 | 16 | 1 | 1.71x slower |

The tradeoff is confined to callers that poll hydration status; the public
status now provides actionable progress and an ETA instead of only a lifecycle
state. Raw samples are in [BENCHMARK.md](BENCHMARK.md#m224-hydration-progress).

## Verification

```text
make m224-test
make m224-race
make m224-vet
make m224-benchmark
```

The focused test pauses a 512-row build at 256 rows, verifies the half-way
frontier and positive estimate, then verifies ready state at 512/512.
