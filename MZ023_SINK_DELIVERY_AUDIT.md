# MZ-23 Sink Delivery Audit

`SQLSinkCommitCoordinator` already prevents duplicate sink commits and tracks
the source frontier attached to each commit. This optional audit adds a
bounded, payload-free history of terminal delivery outcomes so operators can
tell a successful delivery from a retry, conflict, or failed callback.

## Usage

```go
audit := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{
	Capacity: 1024,
})
coordinator := hatSql.NewSQLSinkCommitCoordinatorWithOptions(
	hatSql.SQLSinkCommitCoordinatorOptions{Audit: audit},
)

committed, err := coordinator.Commit(hatSql.SQLSinkCommit{
	Sink:          "warehouse",
	TransactionID: "txn-42",
	Progress: []hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 120},
	},
}, func() error {
	return publishToWarehouse()
})
_ = committed
_ = err

checkpoint := audit.Snapshot()
payload, err := audit.MarshalBinary()
restoredCheckpoint, err := hatSql.UnmarshalSQLSinkDeliveryAudit(payload)
restored, err := hatSql.NewSQLSinkDeliveryAuditFromSnapshot(restoredCheckpoint)
_ = checkpoint
_ = restored
```

The coordinator records `committed`, `failed`, `duplicate`, and `conflict`
outcomes. Each event carries the sink transaction ID and a sorted copy of all
acknowledged source partitions. Callback error text is retained only for a
failed outcome and is bounded to 4 KiB. Sink payloads are never retained.

`Capacity` defaults to 256 when zero or negative and is capped at 65,536.
When the ring is full, the oldest event is dropped and `Stats().Dropped`
increments. Snapshot restore validates event ordering, sink/partition
ownership, outcome values, field lengths, and the binary framing before
publishing replacement state.

The feature is opt-in. `NewSQLSinkCommitCoordinator()` continues to create a
coordinator without an audit pointer, and no audit storage is allocated unless
the caller constructs and supplies one.

## Measurements

Commands:

```text
make benchmark-mz023-baseline
make benchmark-mz023
```

Both runs used `GOMAXPROCS=1`, `-benchtime=100ms`, `-count=5`, and
`-benchmem` on the same host. The clean baseline median for a new commit was
919 ns/op, 5 allocs/op, and 427 B/op. The no-audit feature path measured a
965.5 ns/op median, with the same 5 allocs/op; the samples varied from
736.0 to 1,106 ns/op, so this small difference is treated as benchmark noise
and not as a claimed speed improvement.

The explicitly audited commit path measured a 1,179 ns/op median, 7
allocs/op, and 485 B/op in the same five-run sample set. That is the bounded
cost of retaining one terminal event with one source partition. Binary
checkpoint encoding for 64 events measured 20.9-22.3 microseconds, 30,592 B/op,
and 195 allocs/op. These costs apply only when auditing or exporting is
enabled; the default coordinator does not retain event history.
