# SQL Sink Progress

`hatSql.SQLSinkProgressTracker` stores monotone acknowledged frontiers for
sink partitions. It is independent from source ingestion progress and does
not retain sink payloads or delivery records.

```go
tracker := hatSql.NewSQLSinkProgressTracker()
_, err := tracker.Acknowledge(hatSql.SQLSinkProgress{
	Sink: "warehouse",
	Partition: "0",
	Frontier: 120,
})
```

Use `Acknowledge` for one sink partition or `AcknowledgeBatch` for a group.
Batch input is fully validated before any update is applied, so invalid or
duplicate input cannot partially change the tracker. Valid stale entries are
no-ops; newer entries advance independently. `Snapshot` and `Restore` provide
deterministic, independently owned checkpoint data.

The tracker stores only one `uint64` per sink/partition pair. It does not
provide exactly-once delivery, durable sink commits, retries, or payload
deduplication; those concerns remain part of the later M047 contract and the
sink implementation.

## Cost

Measured on AMD Ryzen 9 5950X, Linux/amd64, two sink partitions per operation,
three one-second Go benchmark samples:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `AcknowledgeBatch` | 193.9 | 80 | 1 |
| Two separate `Acknowledge` calls | 115.9 | 0 | 0 |

The batch path intentionally pays validation and normalization cost to reject
bad groups before mutation. Use individual acknowledgements when partitions
are independent and batch validation is unnecessary.
