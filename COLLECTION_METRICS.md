# Collection Size And Compaction Metrics

`hatMetrics.CollectionMetricsRegistry` records the current size of each
collection and the number of completed compactions. It provides a small
ClickHouse-style collection/part observability boundary without retaining the
collection implementation or changing its storage behavior.

```go
registry := hatMetrics.NewCollectionMetricsRegistry()
_ = registry.SetSize("orders", 1000, 65536)
_ = registry.RecordCompaction("orders")
_ = registry.SetSize("orders", 800, 49152) // gauges may decrease

rows := registry.Snapshot()
// [{Collection:"orders", Entries:800, Bytes:49152, CompactionsTotal:1}]
```

Collection names are trimmed and must be non-empty. `SetSize` replaces the
entry and byte gauges, while `RecordCompaction` increments the counter and
creates a zero-sized row if needed. Snapshots are sorted by collection name,
independently owned, and safe to export while reporters update the registry.
The registry is in-memory metrics state only.
