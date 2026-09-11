# Typed Table Arrangement Telemetry

`hatSql.TypedTableAggregateArrangements` exposes an explicit, read-only
`Stats()` method for diagnosing shared aggregate arrangements. It reports
freshness, retained-state estimates, cardinality, and dictionary group-order
rebuilds without materializing aggregate result rows.

The API is opt-in. Creating a typed table or arrangement does not start a
monitor, allocate a telemetry collector, or add work to ordinary updates.

## Usage

```go
package main

import "hatrie_cache/hat/hatSql"

func inspect(table *hatSql.TypedTable) {
	arrangements, err := hatSql.NewTypedTableAggregateArrangements(table)
	if err != nil {
		return
	}
	arrangement, err := arrangements.Acquire(hatSql.TypedTableAggregateDefinition{
		GroupBy:      []string{"region"},
		DistinctField: "user_id",
	})
	if err != nil {
		return
	}
	defer arrangement.Release()

	registryStats := arrangements.Stats()
	for _, detail := range registryStats.Arrangements {
		_ = detail.DefinitionKey
		_ = detail.EstimatedBytes
		_ = detail.DistinctValues
	}

	leaseStats, err := arrangement.Stats()
	if err != nil {
		return
	}
	_ = leaseStats.Checkpoint
}
```

`Stats()` on the registry returns arrangements sorted by `DefinitionKey` and
copies the result slice, so callers may retain or mutate the returned report.
`Stats()` on an individual lease returns the same detail without the registry
list. A released lease returns an error.

## Fields

| Field | Meaning |
|---|---|
| `SourceSequence` | Latest committed source-table change sequence observed by the report. |
| `ActiveDefinitions` | Number of distinct aggregate definitions currently retained. |
| `ActiveLeases` | Total references held by callers across all definitions. |
| `DefinitionKey` | Stable internal key identifying the normalized aggregate definition. |
| `References` | Number of leases sharing that definition's aggregate state. |
| `Checkpoint` | Last source change sequence applied to the aggregate. |
| `CompactedThrough` | Source changelog watermark; hydration before this sequence can no longer replay retained changes. It is a watermark, not a count of compaction calls. |
| `Groups` | Number of live aggregate groups. |
| `DistinctValues` | Total retained distinct values across groups when `DistinctField` is configured; otherwise zero. |
| `CompactionCount` | Number of dictionary-backed ordered group-list rebuilds. Updates invalidate that list; repeated reads while it is current do not increment the count. Legacy non-dictionary arrangements report zero. |
| `EstimatedBytes` | Bounded estimate of retained aggregate metadata, keys, typed values, dictionary state, and auxiliary indexes. |

`EstimatedBytes` is an accounting estimate, not a reading from the Go
allocator or `runtime.MemStats`. It intentionally avoids forcing group-key or
result-row materialization. Use process/runtime metrics for total heap usage.

## Synchronization

The registry report takes the arrangement registry lock and each shared
aggregate lock while copying counters. The individual report takes only the
lease's shared aggregate lock plus a short source-table read lock. The report
does not mutate table, changefeed, or aggregate state.

Call it from a diagnostics, health, or operator endpoint at a bounded cadence;
do not put it in a per-row update loop. The registry call is linear in the
number of active definitions and the retained groups because it computes the
memory and distinct-value estimates.

## Measurement

The controlled benchmark is run with:

```text
make benchmark-mz027-arrangement-stats
```

On Linux `amd64` with an AMD Ryzen 9 5950X, five one-second samples of
`BenchmarkMZ027ArrangementStats` measured the new stats call at a median of
220.1 ns/op, 104 B/op, and 2 allocations per call. The existing arrangement benchmark remained at 383 allocations per
operation in both independent and shared modes. The detailed before/after
samples and the interpretation are recorded in [BENCHMARK.md](BENCHMARK.md#mz-027-arrangement-memory-telemetry).
