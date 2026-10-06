# Conflict Introspection Stream

`hat/hatReplication.ConflictEventLog` is an opt-in bounded stream of
conflict decisions. It keeps the conflict source IDs and decision, but stores
only a truncated SHA-256 digest of the application key. It can stay in memory
or use `FileConflictEventStore` for a checksummed, atomically replaced HCE1
snapshot.

The log is deliberately not wired into every resolver by default. Callers that
need audit or replay diagnostics opt in at the point where a conflict policy is
applied:

```go
log, err := hatReplication.NewConflictEventLog(
	hatReplication.ConflictEventLogOptions{Capacity: 1024},
)
if err != nil {
	return err
}

winner, err := policy.ResolveAndRecord(
	log,
	"orders",
	orderKey,
	leftVersion,
	rightVersion,
)
if err != nil {
	return err
}
_ = winner

page, err := log.Since(lastSequence, 128)
if err != nil {
	if errors.Is(err, hatReplication.ErrConflictEventGap) {
		// Refresh from Snapshot before resuming the cursor.
	}
	return err
}
```

For restart retention, provide a file store:

```go
store, err := hatReplication.NewFileConflictEventStore(
	"/var/lib/hatrie/conflicts.hce",
)
if err != nil {
	return err
}
log, err := hatReplication.NewConflictEventLog(
	hatReplication.ConflictEventLogOptions{
		Capacity: 1024,
		Store:    store,
	},
)
```

## Semantics

- `ResolveAndRecord` records left-wins, right-wins, and rejected decisions;
  equal versions are not conflicts and produce no event.
- Every event has a strictly increasing sequence. `Since` returns a bounded
  page and reports `ErrConflictEventGap` when retention has evicted the
  requested cursor range, preventing silent loss.
- The default capacity is 1,024 events and the hard maximum is 4,096. Page
  size is bounded at 512 events.
- HCE1 encodes source IDs, decisions, sequence numbers, digests, retention
  counters, and a CRC32C checksum. Raw keys never enter the event or file.
- File snapshots use mode `0600`, a private temporary file, file sync, rename,
  and parent-directory sync. A failed save rolls the in-memory append back.
- The log is not a consensus or authorization system. Protect the file path,
  choose the retention window, and decide which source IDs may be exposed.

## Benchmark

These are five 200 ms runs on the same host, with `-benchmem`:

| Operation | Median | Memory | Interpretation |
| --- | ---: | ---: | --- |
| Existing conflict resolution | 11.32 ns/op | 0 B/op, 0 allocs/op | Current baseline |
| Existing conflict resolution, parallel | 28.38 ns/op | 0 B/op, 0 allocs/op | Current baseline |
| `ResolveAndRecord`, in memory | 1,108 ns/op | 0 B/op, 0 allocs/op | Opt-in conflict path |
| Event record, parallel | 1,204 ns/op | 0 B/op, 0 allocs/op | Bounded mutex path |
| `Since`, 32-event page | 1,187 ns/op | 2,688 B/op, 1 alloc/op | Returned page copy |
| Encode 64-event snapshot | 2,927 ns/op | 3,200 B/op, 1 alloc/op | Before optional file I/O |

The event log is not a replacement for the existing resolver and adds no
work unless a caller constructs it and chooses `ResolveAndRecord`. Its cost is
therefore an explicit audit/replay tradeoff rather than a regression to normal
conflict resolution.

The reproducible target is:

```text
make codex-tu38-bench
```
