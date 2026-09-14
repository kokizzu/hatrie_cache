# MZ-043 Operator Frontier Lag Metrics

This adds an opt-in Materialize-inspired progress view for independent
operators. It answers which operator is behind the observed frontier, instead
of exposing only a source-level frontier or retained-memory gauge.

## API

```go
frontiers := hatMetrics.NewOperatorFrontierRegistry()
_ = frontiers.Advance("orders-scan", 120)
_ = frontiers.Advance("orders-sort", 117)

handler := hatCache.NewMonitoringHandler(trie, hatCache.MonitoringOptions{
    NodeName:                 "node-a",
    OperatorFrontier:         frontiers,
    OperatorFrontierObserved: func() uint64 { return 120 },
})
```

`Advance` trims the operator name, accepts equal updates, and rejects a
regression with `hatMetrics.ErrOperatorFrontierRegressed`. The zero value is
usable. `Snapshot` is deterministic and sorted by operator name. Call
`Delete` when an operator is removed or rebuilt so its state and metric series
are released.

The observed callback is supplied by the application because the registry does
not know the source or pipeline's global observation point. Without the
callback, only the current operator frontier gauges are emitted and lag is not
exported.

## Prometheus Metrics

Metrics are absent by default. When configured, `/metrics` emits:

```text
hatrie_cache_operator_frontier{node="node-a",operator="orders-scan"} 120
hatrie_cache_operator_observed{node="node-a"} 120
hatrie_cache_operator_lag{node="node-a",operator="orders-sort"} 3
```

Operator names become Prometheus label values. Use stable, bounded operator
names rather than request IDs, tenant IDs, or other unbounded user input.

This registry is derived operational state. It is not persisted in snapshots,
backups, or replication streams; repopulate it after restore or process
restart.

## Cost And Limits

The default path has no registry, callback, snapshot, or metric-series cost.
With 128 operators, a metrics scrape exports 256 per-operator gauges plus one
observed gauge. The benchmark measured about 25,229 response bytes versus
6,572 bytes with the exporter disabled. This bandwidth and scrape work scales
with the number of configured operators, so the feature should remain opt-in
and should use a reasonable scrape interval.

The registry itself uses the same map plus sorted-snapshot approach as the
existing source-frontier registry. Its measured update path was allocation-free
and its snapshot used 7,680 bytes and two allocations for 128 operators. See
the raw five-sample results in [BENCHMARK.md](BENCHMARK.md#mz-043-operator-frontier-lag).
