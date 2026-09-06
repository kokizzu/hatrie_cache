# Operator Retained-Memory Metrics

`hatMetrics.OperatorMemoryRegistry` stores the latest retained-byte gauge for
each named operator. It is intended for Materialize-style operator-level
observability: a reporter can publish current memory after a batch, compaction,
or spill event without retaining the operator itself.

```go
registry := hatMetrics.NewOperatorMemoryRegistry()
_ = registry.Set("scan", 4096)
_ = registry.Set("group-by", 8192)
_ = registry.Set("group-by", 2048) // gauges may decrease after compaction

rows := registry.Snapshot()
// [{Operator:"group-by", RetainedBytes:2048},
//  {Operator:"scan", RetainedBytes:4096}]
```

Names are trimmed and must be non-empty. `Set` replaces the previous value,
including when memory decreases. `Snapshot` returns independent rows sorted by
operator name, making exports deterministic. The registry is concurrency-safe
and in-memory only; it does not alter query scheduling or memory limits.
