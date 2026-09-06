# Operator Memory Monitoring

Operator retained-memory metrics are optional. Set
`MonitoringOptions.OperatorMemory` to a `hatMetrics.OperatorMemoryRegistry`
and report each operator's retained bytes through `Set`.

When configured, `/metrics` exposes the deterministic gauge
`hatrie_cache_operator_retained_memory_bytes{node,operator}`. Operator names
are emitted in sorted order and Prometheus label escaping is applied. When the
option is nil, the endpoint emits no operator-memory metrics and the existing
monitoring output is unchanged.
