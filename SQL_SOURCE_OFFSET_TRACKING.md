# SQL Source Offset Tracking

`hatSql.SQLSourceOffsetTracker` stores one monotone high-watermark per
`Source` and `Partition`. It is intended for Kafka-style source consumers that
need a compact checkpoint independent of the source payload and table row
state.

## Usage

Advance the tracker only after the corresponding source records have been
successfully applied to the table or downstream state:

```go
tracker := hatSql.NewSQLSourceOffsetTracker()

// Consume records from events/0 and apply them first.
if err := applyEvents(events); err != nil {
	return err
}
advanced, err := tracker.Advance(hatSql.SQLSourceOffset{
	Source:    "events",
	Partition: "0",
	Offset:    lastAppliedOffset,
})
if err != nil {
	return err
}
if advanced {
	// Persist tracker.Snapshot() with the consumer checkpoint.
}
```

`AdvanceBatch` is useful when a consumer finishes one batch across multiple
partitions:

```go
advanced, err := tracker.AdvanceBatch([]hatSql.SQLSourceOffset{
	{Source: "events", Partition: "0", Offset: 104},
	{Source: "events", Partition: "1", Offset: 98},
})
```

`advanced` counts entries newer than their current high-watermarks. Older or
replayed offsets are valid no-ops. `Offset` reads one position and `Snapshot`
returns deterministic source/partition order for persistence. `Restore`
atomically replaces the full tracker state and accepts an empty snapshot.

## Contract

- Source and partition names are trimmed and must both be non-empty.
- Offset zero is valid.
- An offset is accepted only when it is greater than the current offset for
  that source/partition. The tracker does not require contiguous offsets,
  since consumers may intentionally skip filtered records.
- `AdvanceBatch` rejects duplicate source/partition entries before changing
  any state. Invalid input and duplicate restore entries also leave state
  unchanged.
- The tracker is safe for concurrent `Advance`, `AdvanceBatch`, `Offset`,
  `Snapshot`, and `Restore` calls.
- `Snapshot` contains only high-watermarks; it does not retain source records.
- This is checkpoint metadata, not exactly-once processing. It does not
  atomically commit a table mutation, broker offset, and external snapshot.
  Apply source records first, then advance and persist the checkpoint. The
  exactly-once source/sink guarantees remain a separate design concern.

## Benchmark

Run:

```text
make benchmark-m043-clean
```

The benchmark repeatedly advances 256 pre-created partitions (AMD Ryzen 9
5950X, Linux/amd64):

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| `Advance` | 71.28 | 0 | 0 |

This is an absolute tracker cost measurement; there was no prior public
high-watermark API to use as a compatible baseline.
