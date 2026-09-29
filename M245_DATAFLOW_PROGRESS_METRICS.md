# M245 Dataflow Progress Metrics

This is a Materialize-inspired, opt-in progress API for the existing bounded
`hatSql.SQLDataflowMetricsCatalog`. It exposes the relationship between an
input frontier and an output frontier without adding work to ordinary SQL
queries or starting a background worker.

## API

```go
catalog, err := hatSql.NewSQLDataflowMetricsCatalog(
	 hatSql.SQLDataflowMetricsCatalogOptions{},
)
if err != nil {
	panic(err)
}

inputAt := time.Unix(100, 0).UTC()
outputAt := inputAt.Add(250 * time.Millisecond)
err = catalog.ObserveProgress(hatSql.SQLDataflowObjectSource, "orders", hatSql.SQLDataflowProgressObservation{
	InputTimestamp:   1000,
	OutputTimestamp:  990,
	InputObservedAt:  inputAt,
	OutputObservedAt: outputAt,
})
if err != nil {
	panic(err)
}

progress, err := catalog.Progress(hatSql.SQLDataflowObjectSource, "orders")
if err != nil {
	panic(err)
}
// progress.TimestampLag == 10
// progress.InputToOutputLatency == 250*time.Millisecond
```

The first `ObserveProgress` call creates one bounded catalog object with six
standard metric points. Later calls update those points in place. `Progress`
returns exact `uint64` logical timestamps and the derived values; `Rows` and
`Snapshot` expose the same measurements through the existing catalog paths.

## Metrics

| Name | Unit | Meaning |
| --- | --- | --- |
| `input_timestamp` | `timestamp` | Current input frontier. |
| `output_timestamp` | `timestamp` | Current output frontier. |
| `input_output_timestamp_lag` | `timestamp` | Saturated input minus output frontier distance. |
| `input_timestamp_throughput` | `timestamp/s` | Input timestamp delta divided by elapsed input observation time. |
| `output_timestamp_throughput` | `timestamp/s` | Output timestamp delta divided by elapsed output observation time. |
| `input_to_output_latency` | `s` | Current output wall-clock observation time minus input wall-clock observation time. |

Throughput is zero until two non-zero wall-clock observations are available.
Logical timestamps and observation times must be monotone. A malformed update
is rejected before catalog state changes. If wall-clock fields are omitted,
frontier and lag metrics still work while derived rates and latency remain
zero. `TimestampLag` saturates at zero when an output frontier is ahead of an
input frontier.

## Bounds And Lifecycle

The six standard points count against `MaxObjects`, `MaxMetricsPerObject`, and
`MaxMetricPoints`. Existing catalog validation still bounds object names and
metric names. An `Upsert` replaces an object and clears its previous progress
snapshot; `Remove` clears both the object and its progress state. No progress
state is allocated until the first object is observed.

The API is opt-in. Existing catalog construction, `UpdateMetric`, SQL query
execution, persistence, wire formats, and default server configuration are
unchanged. The steady-state progress update is allocation-free in the measured
path.

## Cost

The benchmark compares six generic metric updates, each taking the catalog
lock and validating a metric name, with one typed progress update that computes
all six values under one lock. On the benchmark host, the median was 602.4
ns/op versus 228.6 ns/op, or 2.63x lower update time. Both paths measured
0 B/op and 0 allocs/op. See [BENCHMARK.md](BENCHMARK.md#m245-dataflow-progress-metrics)
for every sample and the exact command.

The comparison is specifically the cost of publishing a complete six-metric
progress sample, not a claim that all SQL queries become 2.60x faster. The
tradeoff for an enabled object is six bounded metric points plus one compact
last-progress snapshot.

Run `make verify-m245` for package tests, race tests, and vet, and
`make benchmark-m245` for the reproducible five-sample comparison.
