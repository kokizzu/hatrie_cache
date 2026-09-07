# SQL Source Transaction Grouping

`hatSql.SQLSourceOffsetTracker.AdvanceTransaction` groups source partition
high-watermarks into one atomic progress update.

```go
tracker := hatSql.NewSQLSourceOffsetTracker()
advanced, err := tracker.AdvanceTransaction(hatSql.SQLSourceTransaction{
	ID: "txn-42",
	Offsets: []hatSql.SQLSourceOffset{
		{Source: "orders", Partition: "0", Offset: 120},
		{Source: "orders", Partition: "1", Offset: 87},
	},
})
```

The transaction ID must be non-empty and offsets must contain distinct source
and partition pairs. Every offset must be newer than its current watermark for
the group to advance. If one member is stale, the method returns `(false,
nil)` and changes no member. Invalid input returns a validation error.

The tracker does not retain transaction IDs or source payloads. This keeps
memory bounded by the number of tracked partitions, but it means this API is
not an exactly-once transaction ledger. Durable transaction deduplication and
source commit acknowledgement remain a separate M045 concern.

## Cost

Measured on AMD Ryzen 9 5950X, Linux/amd64, Go benchmark mode, two offsets per
operation, three one-second samples:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `AdvanceTransaction` | 207.2 | 80 | 1 |
| Two separate `Advance` calls | 111.4 | 0 | 0 |

The grouped call intentionally costs more because it validates the whole
group and enforces all-or-nothing semantics. It should be used when atomic
cross-partition progress matters; use `Advance` for independent offsets.
