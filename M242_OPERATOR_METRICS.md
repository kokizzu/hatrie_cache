# M242 Per-Operator Metrics

This is a Materialize-inspired, opt-in metric path for operators in a
dataflow. It maintains cumulative update and batch counts plus the operator's
logical frontier in the existing bounded `hatSql.SQLDataflowMetricsCatalog`.
It does not add work to ordinary SQL execution or start a worker.

## API

```go
catalog, err := hatSql.NewSQLDataflowMetricsCatalog(
	hatSql.SQLDataflowMetricsCatalogOptions{},
)
if err != nil {
	panic(err)
}

err = catalog.ObserveOperator(hatSql.SQLDataflowObjectCompute, "join", hatSql.SQLDataflowOperatorObservation{
	Updates:    120_000,
	Batches:    400,
	Frontier:   900,
	ObservedAt: time.Now().UTC(),
})
if err != nil {
	panic(err)
}

snapshot, err := catalog.Operator(hatSql.SQLDataflowObjectCompute, "join")
if err != nil {
	panic(err)
}
// snapshot.Updates, snapshot.Batches, and snapshot.Frontier are exact uint64s.
```

The first observation creates three bounded metric points. Subsequent
observations update fixed slots in place. `Operator` returns exact counter and
frontier values; `Rows` and `Snapshot` expose the same points through the
existing catalog views.

## Metrics And Semantics

| Name | Unit | Meaning |
| --- | --- | --- |
| `operator_updates_total` | `updates` | Cumulative updates accepted by the operator. |
| `operator_batches_total` | `batches` | Cumulative input/output batches processed by the operator. |
| `operator_frontier` | `timestamp` | Current logical frontier for the operator. |

All three values are monotone. A regression is rejected without changing
catalog state. `ObservedAt` is optional and is only used as the latest
observation timestamp; operators can omit it when they do not have a wall
clock. Use `SQLDataflowObjectCompute` for ordinary dataflow operators; the
catalog kind remains explicit for callers that maintain source or sink stages.

## Bounds And Lifecycle

The three standard points count against `MaxObjects`, `MaxMetricsPerObject`,
and `MaxMetricPoints`. Existing catalog validation bounds names and metric
cardinality. `Upsert` replaces an object and clears its operator snapshot;
`Remove` clears both the object and operator state. Operator state is allocated
only after the first observation.

The feature is disabled by default. Existing catalog construction,
`UpdateMetric`, SQL execution, persistence, wire formats, and server defaults
are unchanged. The steady-state typed update is allocation-free in the
measured path.

## Measurement

The benchmark compares three generic `UpdateMetric` calls with one typed
`ObserveOperator` call publishing the same three values. The latest five-sample
median was 255.2 ns/op for the generic path versus 137.0 ns/op for M242, a
1.86x lower update time. Both paths measured 0 B/op and 0 allocs/op. See
[BENCHMARK.md](BENCHMARK.md#m242-per-operator-metrics) for raw samples and
`make benchmark-m242`.

This measures metric publication, not arbitrary SQL query speed. The enabled
operator retains three bounded metric points and one compact exact snapshot.
Run `make verify-m242` for package tests, race tests, and vet.
