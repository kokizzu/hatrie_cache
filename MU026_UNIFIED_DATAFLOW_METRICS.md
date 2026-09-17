# M-U26 Unified Dataflow Metrics

`hat/hatSql.SQLDataflowMetricsCatalog` is an opt-in, importable catalog for
metrics owned by SQL dataflow sources, compute stages, and sinks. It gives
callers one bounded object model and a deterministic SQL-shaped row view.

The catalog does not start a server, change the monitoring-server default, or
automatically expose an HTTP endpoint. A service can register `Rows()` with its
existing SQL resolver or monitoring adapter when it wants this data published.
The existing `SQLTelemetry` observer remains the right choice for query-wide
counters and Prometheus/OpenTelemetry export.

## Example

```go
package main

import (
	"time"

	"hatrie_cache/hat/hatSql"
)

func main() {
	catalog, err := hatSql.NewSQLDataflowMetricsCatalog(hatSql.SQLDataflowMetricsCatalogOptions{})
	if err != nil {
		panic(err)
	}

	updatedAt := time.Now().UTC()
	for _, object := range []hatSql.SQLDataflowMetricsObject{
		{
			Kind: hatSql.SQLDataflowObjectSource,
			Name: "orders",
			Metrics: []hatSql.SQLDataflowMetricPoint{
				{Name: "rows_read", Unit: "rows", Value: 120, UpdatedAt: updatedAt},
			},
		},
		{
			Kind: hatSql.SQLDataflowObjectCompute,
			Name: "daily_rollup",
			Metrics: []hatSql.SQLDataflowMetricPoint{
				{Name: "rows_out", Unit: "rows", Value: 118, UpdatedAt: updatedAt},
			},
		},
		{
			Kind: hatSql.SQLDataflowObjectSink,
			Name: "warehouse",
			Metrics: []hatSql.SQLDataflowMetricPoint{
				{Name: "rows_sent", Unit: "rows", Value: 118, UpdatedAt: updatedAt},
			},
		},
	} {
		if err := catalog.Upsert(object); err != nil {
			panic(err)
		}
	}

	// After the shape is registered, use the allocation-free success path for
	// every producer update.
	_ = catalog.UpdateMetric(
		hatSql.SQLDataflowObjectSource,
		"orders",
		"rows_read",
		121,
		time.Now().UTC(),
	)

	rows := catalog.Rows()
	_ = rows // pass rows to the application's SQL resolver or exporter
}
```

The row columns are:

| Column | Go value | Meaning |
|---|---|---|
| `object_kind` | `string` | `source`, `compute`, or `sink` |
| `object_name` | `string` | Bounded dataflow object name |
| `metric` | `string` | ASCII metric name |
| `value` | `float64` | Finite numeric value |
| `unit` | `string` | Optional bounded unit, such as `rows` or `By` |
| `updated_at` | `time.Time` | Last update time; zero when not supplied |

Rows are ordered by object kind (`source`, `compute`, `sink`), object name, and
metric name. `Snapshot()` returns the same ordering as typed objects. Both
methods return independent data; changing a returned slice, metric, or row map
does not mutate the catalog.

## Update Model

`Upsert` is the shape-management operation. It validates the whole object,
copies every metric, and atomically replaces the prior object. Invalid input or
a bound failure leaves the existing object unchanged.

`UpdateMetric` is the hot path. The metric must already be registered by
`Upsert`; the operation changes only its finite value and timestamp and returns
without allocating on success. Requiring pre-registration keeps cardinality
bounded and avoids silently creating misspelled metric names. Missing objects or
metrics return `ErrSQLDataflowMetricsNotFound`.

`Remove` removes a complete object and releases its metric-point count.
`Stats()` reports the current object and metric-point counts.

## Defaults And Bounds

An all-zero options value selects these defaults:

| Option | Default |
|---|---:|
| `MaxObjects` | 1,024 |
| `MaxMetricsPerObject` | 64 |
| `MaxMetricPoints` | 65,536 |
| `MaxObjectNameBytes` | 256 |
| `MaxMetricNameBytes` | 128 |
| `MaxUnitBytes` | 32 |

Positive options can lower or raise these values within the implementation's
hard safety ceilings. Negative values and excessively large configurations are
rejected with `ErrSQLDataflowMetricsInvalid`. An object update that would exceed
a live bound returns `ErrSQLDataflowMetricsLimit`.

Object and unit text must be valid UTF-8 without control characters. Metric
names are restricted to ASCII letters, digits, `_`, `-`, and `.`. Duplicate
metric names, empty object or metric names, and NaN or infinity values are
rejected. There is no arbitrary label map or payload field, which keeps output
cardinality and accidental sensitive-data retention under caller control.

## Operational Guidance

- Register only stable source, compute, and sink names. Do not use request IDs,
  user IDs, or unbounded values as object or metric names.
- Call `UpdateMetric` from producers after one startup `Upsert` per shape.
- Call `Rows()` for a bounded scrape or SQL snapshot. It intentionally creates
  independent row maps, so callers must not use it as a per-event hot path.
- Use `Snapshot()` when a typed Go view is sufficient and avoid row-map
  conversion overhead.
- Keep this catalog opt-in. It has no effect on the existing query telemetry,
  HTTP/2, gRPC, or monitoring-server lifecycle until the caller wires it in.

## Verification

Focused correctness, invalid-input, replacement-atomicity, bound, removal, and
concurrency tests run with:

```sh
make test-mu026-unified-metrics
make race-mu026-unified-metrics
make vet-mu026-unified-metrics
```

The paired benchmark and its synthetic split-map control are documented in
[BENCHMARK.md](BENCHMARK.md#mu-026-unified-dataflow-metrics).
