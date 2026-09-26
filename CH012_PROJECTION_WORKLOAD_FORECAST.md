# CH-012 Projection Workload Forecast

This change adopts a bounded workload-forecasting idea from ClickHouse-style
workload-aware projection selection. `SQLProjectionAdvisor` can now retain a
counter plus the first and last observation time for each query/dependency/
shape candidate and project that rate over a caller-selected horizon.

## Usage

Forecasting is disabled by the existing constructor and must be enabled
explicitly:

```go
advisor := hatSql.NewSQLProjectionAdvisorWithOptions(hatSql.SQLProjectionAdvisorOptions{
    Capacity:               128,
    EnableWorkloadForecast: true,
})

forecasts, err := advisor.ForecastWorkloadAt(time.Now(), 24*time.Hour)
```

Applications that observe work outside the normal SQL execution path can feed
the advisor directly:

```go
advisor.RecordWorkload("daily-region", []string{"events"}, time.Now())
```

When the opt-in advisor is attached to `QueryOptions`, the existing uncached
SQL execution hook records successful eligible queries automatically. The
explicit method remains available for early-return paths and external query
layers.

The forecast is a conservative rate estimate:

- one observation forecasts one query;
- multiple observations use `observed_count * forecast_window /
  observation_window`, rounded up;
- arithmetic saturates at `math.MaxUint64`;
- SQL text, literals, rows, and individual events are never retained;
- the workload map is bounded by `Capacity` and the default constructor adds
  no workload map or per-query tracking.

Forecast counts can also drive the existing cost model without changing the
legacy cost API:

```go
recommendations, err := advisor.ForecastCostBasedRecommendations(
    32,
    time.Now(),
    24*time.Hour,
    hatSql.SQLProjectionCostModel{
        QueryHitLatency:  100 * time.Microsecond,
        InitialBuildCost: 10 * time.Millisecond,
        RefreshCost:      time.Millisecond,
    },
)
```

`ForecastCostBasedRecommendations` is explicit and opt-in. It ranks only
observed forecast candidates, uses each candidate's projected query count, and
still requires the caller to supply hit, build, and refresh costs. The
`ExpectedQueries` field is not needed on this path; `ExpectedRefreshes` remains
available for maintenance cost. A disabled forecast advisor returns an empty
result, and neither this method nor the legacy cost method creates a
projection or changes SQL execution.

Forecast counters are process-local and are not added to the existing
recommendation snapshot format. Callers that need restart continuity should
replay their own aggregate observations after restore.

## Benchmark

Machine: AMD Ryzen 9 5950X, linux/amd64. Each row is five `go test -benchmem`
runs; the middle value is the median. The baseline is the committed `HEAD`
before this feature, archived into a clean temporary worktree.

| Benchmark | Before median | After median | Allocations | Result |
| --- | ---: | ---: | ---: | --- |
| Existing cost recommendations, 128 candidates | 30,975 ns/op | 31,943 ns/op | 29,576 B/op, 132 allocs/op | Same memory; +3.1% in this run, not a measured feature win |
| Default slow-feedback update | not present in baseline benchmark | 169.8 ns/op | 16 B/op, 2 allocs/op | Default path remains unchanged |
| Opt-in workload recording | not applicable | 141.8 ns/op | 16 B/op, 2 allocs/op | Opt-in counter update |
| Forecast 128 candidates | not applicable | 38,407 ns/op | 29,576 B/op, 132 allocs/op | Includes result construction and deterministic sort |

Raw after-run ranges:

- existing cost recommendations: `31,574`, `31,597`, `31,943`, `32,281`,
  `32,281 ns/op`;
- default feedback: `167.0`, `167.5`, `169.8`, `169.9`, `170.2 ns/op`;
- workload recording: `141.0`, `141.3`, `141.8`, `142.2`, `142.5 ns/op`;
- forecast generation: `38,246`, `38,393`, `38,407`, `38,436`, `39,478 ns/op`.

The forecast-aware cost path was measured separately over 128 candidates while
returning 32 recommendations. Its raw samples were `36,599`, `35,779`,
`35,906`, `36,554`, and `39,240 ns/op`, with `29,576 B/op` and `132 allocs/op`.
The clean pre-change cost baseline was `31,682 ns/op`, `29,576 B/op`, and
`132 allocs/op`; the forecast-aware path is `1.15x` that CPU cost but has the
same heap and allocation profile. This is an explicit selection operation, not
an ordinary query-path cost.

The existing cost path has the same allocation count and retained bytes. Its
small latency difference is not attributable to a changed loop and should be
rechecked if this path becomes a performance gate; the new functionality is
opt-in and does not change recommendation ordering or query execution.

## Verification

Passed:

- `make test-ch012-projection-forecast`
- `make race-ch012-projection-forecast`
- `make test-hatsql-package`
- `make benchmark-ch012-projection-forecast`

The repository-wide `make test` was blocked by the shared Go build cache being
unreadable. A rerun with an isolated temporary `GOCACHE` compiled and ran the
repository packages, but existing `hatCache` assertion failures remained and
the aggregate run later stopped producing output; it was terminated. Those
failures are outside this CH-012 change.
