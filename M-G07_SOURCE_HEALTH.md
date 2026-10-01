# M-G07 Source Health Records

This adopts a small part of Materialize-style source health reporting: source
connectors can publish bounded health state alongside their progress frontier.
The state is diagnostic and in-memory; it is not part of cache values,
snapshots, backups, or replication payloads.

## API

```go
health := hatMetrics.NewSourceHealthRegistry(1024)
_ = health.Record("orders", hatMetrics.SourceHealthHealthy, 120, "")
_ = health.Record("payments", hatMetrics.SourceHealthDegraded, 117, "upstream timeout")

handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
    SourceHealth: health,
    SourceHealthObserved: func() uint64 { return observedFrontier() },
})
```

`SourceHealthRegistry.Record` accepts `unknown`, `healthy`, `degraded`, and
`failed`. Source names are trimmed and required. Frontiers are monotone per
source. A healthy record clears the consecutive-failure streak and error text;
degraded and failed records increment the saturating streak; unknown records
do not increment it. Error text is trimmed and capped at 1,024 bytes.

The registry is bounded. A non-positive capacity uses
`DefaultSourceHealthCapacity` (1,024); requests above
`MaxSourceHealthCapacity` (1,048,576) are clamped. New source names beyond the
selected capacity return `ErrSourceHealthCapacity`. `Snapshot` returns sorted,
independently owned rows and computes lag from the supplied observed frontier.

## Metrics

Health metrics are disabled when `MonitoringOptions.SourceHealth` is nil. When
enabled, `/metrics` emits:

- `hatrie_cache_source_health_status{node,source,status}` with one active
  status series per source;
- `hatrie_cache_source_health_frontier{node,source}`;
- `hatrie_cache_source_health_failures{node,source}`;
- `hatrie_cache_source_health_updated_at_unix_nano{node,source}`;
- `hatrie_cache_source_health_observed{node}` and
  `hatrie_cache_source_health_lag{node,source}` when an observed callback is
  configured.

Only the four fixed status values become a label. `LastError` is never emitted
as a metric label or value, and source count is bounded by the registry
capacity. `SourceFrontierObserved` is used as a fallback observed callback so
existing source-frontier integrations can add health records without a second
clock callback.

## Cost And Defaults

No cache write or query path calls `Record`; attaching a nil registry is the
default and emits no additional metrics. The opt-in registry itself has a
small diagnostic cost: compared with the existing frontier-only registry at
1,024 sources, its snapshot retained the same two allocations but measured
about 1.11x CPU and 1.83x bytes. A health record measured about 2.80x the CPU
of a frontier advance because it stores status, failure state, bounded error
text, and a timestamp. These costs are paid only by callers that opt into
health records.

## Verification

```text
make test-mg07-source-health
make test-mg07-source-health-cache
make race-mg07-source-health
make race-mg07-source-health-cache
make vet-mg07-source-health
make vet-mg07-source-health-cache
make benchmark-mg07-source-health-compare
make benchmark-mg07-source-health-cache
```

The tests cover sorting, monotone frontiers, reset behavior, unknown status,
capacity and input validation, nil-by-default metrics, observed-frontier
fallback, Prometheus escaping, and keeping error text out of metrics.

## Raw Benchmark Samples

Linux/amd64, AMD Ryzen 9 5950X, five `-benchmem` samples:

```text
BenchmarkSourceHealthRegistrySnapshot-32       7948  134098 ns/op  108545 B/op  2 allocs/op
BenchmarkSourceHealthRegistrySnapshot-32       9286  135117 ns/op  108544 B/op  2 allocs/op
BenchmarkSourceHealthRegistrySnapshot-32       9375  141123 ns/op  108544 B/op  2 allocs/op
BenchmarkSourceHealthRegistrySnapshot-32       8451  142173 ns/op  108544 B/op  2 allocs/op
BenchmarkSourceHealthRegistrySnapshot-32       7986  139013 ns/op  108544 B/op  2 allocs/op
BenchmarkSourceFrontierRegistrySnapshot-32    9055  127086 ns/op   59392 B/op  2 allocs/op
BenchmarkSourceFrontierRegistrySnapshot-32    9262  127009 ns/op   59392 B/op  2 allocs/op
BenchmarkSourceFrontierRegistrySnapshot-32    9331  125685 ns/op   59392 B/op  2 allocs/op
BenchmarkSourceFrontierRegistrySnapshot-32    8898  124851 ns/op   59392 B/op  2 allocs/op
BenchmarkSourceFrontierRegistrySnapshot-32    9096  123019 ns/op   59392 B/op  2 allocs/op
BenchmarkSourceHealthRegistryRecord-32       15219387  80.37 ns/op  0 B/op  0 allocs/op
BenchmarkSourceHealthRegistryRecord-32       14969077  77.40 ns/op  0 B/op  0 allocs/op
BenchmarkSourceHealthRegistryRecord-32       15435733  77.61 ns/op  0 B/op  0 allocs/op
BenchmarkSourceHealthRegistryRecord-32       14717694  79.25 ns/op  0 B/op  0 allocs/op
BenchmarkSourceHealthRegistryRecord-32       15066140  78.34 ns/op  0 B/op  0 allocs/op
BenchmarkSourceFrontierRegistryAdvance-32    41587310  27.79 ns/op  0 B/op  0 allocs/op
BenchmarkSourceFrontierRegistryAdvance-32    42939430  28.66 ns/op  0 B/op  0 allocs/op
BenchmarkSourceFrontierRegistryAdvance-32    38401927  28.26 ns/op  0 B/op  0 allocs/op
BenchmarkSourceFrontierRegistryAdvance-32    42312993  28.08 ns/op  0 B/op  0 allocs/op
BenchmarkSourceFrontierRegistryAdvance-32    42771136  27.94 ns/op  0 B/op  0 allocs/op
BenchmarkWritePrometheusSourceHealthMetrics-32 660 1723610 ns/op 2298279 B/op 14130 allocs/op
BenchmarkWritePrometheusSourceHealthMetrics-32 727 1705259 ns/op 2298252 B/op 14130 allocs/op
BenchmarkWritePrometheusSourceHealthMetrics-32 684 1714557 ns/op 2298283 B/op 14130 allocs/op
BenchmarkWritePrometheusSourceHealthMetrics-32 697 1705558 ns/op 2298245 B/op 14130 allocs/op
BenchmarkWritePrometheusSourceHealthMetrics-32 684 1754634 ns/op 2298267 B/op 14130 allocs/op
```

The formatter benchmark intentionally includes a 1,024-source Prometheus
payload and is a sizing reference for monitoring scrapes, not a cache mutation
benchmark.
