# T-U38 Redacted Conflict Event Journal

`hatReplication.ConflictEventLog` is an opt-in, bounded journal for conflict
resolution diagnostics. It records a fixed-size key digest, source labels, the
selected decision, and a caller-supplied timestamp. It never accepts or stores
the raw key.

## Example

```go
log, err := hatReplication.NewConflictEventLog(hatReplication.ConflictEventLogOptions{
	Capacity: 4096,
})
if err != nil {
	return err
}

event := hatReplication.ConflictEvent{
	Space:        "orders",
	WinnerSource: "region-apac",
	LoserSource:  "region-eu",
	Decision:     hatReplication.ConflictDecisionLastWriteWins,
	Timestamp:    time.Now().UnixNano(),
}
digest := sha256.Sum256([]byte(orderID))
copy(event.KeyDigest[:], digest[:16]) // keep only a redacted digest

cursor, err := log.Append(event)
if err != nil {
	return err
}

events, err := log.ReadAfter(cursor-1, 100)
```

The digest should be produced by the caller with an appropriate secret or
domain-separated hash when key enumeration would be sensitive. The journal
does not provide a reverse lookup.

## Contract

- The journal is disabled unless a caller constructs it and appends events.
- The default capacity is 1,024 events; callers can select 1 through 65,536.
- Each space, source, and decision label defaults to 256 bytes and is capped at
  4,096 bytes.
- Sequence numbers start at 1 and are monotonic. Once the ring is full, the
  oldest records are dropped.
- `ReadAfter` reports `ErrConflictEventHistoryGap` when a cursor is older than
  the retained window. Consumers should recover from a durable snapshot or a
  caller-owned source of truth.
- `WaitAfter` uses per-waiter buffered notifications and does not start a
  goroutine per consumer. Cancellation returns the context error.
- `MarshalBinary` and `UnmarshalBinary` use the deterministic HCE1 binary
  snapshot format. Snapshot storage, fsync, encryption, and access control stay
  with the caller.
- Restore validates bounds, contiguous sequence numbers, decisions, and all
  fields before replacing live state. Invalid input leaves the current log
  unchanged.

## Verification

```sh
make round66-conflict-test
make round66-conflict-full-test
make round66-conflict-race
make round66-conflict-vet
make round66-conflict-benchmark
```

## Measurements

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | Median ns/op | B/op | Allocs/op | Notes |
| --- | ---: | ---: | ---: | --- |
| Existing conflict resolution | 5.99 | 0 | 0 | clean baseline |
| `ConflictEventLog.Append` | 31.56 | 0 | 0 | opt-in journal, fixed ring |
| `ReadAfter`, 1,024 events | 28,146 | 98,304 | 1 | caller receives a copied batch |
| `MarshalBinary`, 1,024 events | 40,709 | 163,840 | 2 | caller-owned persistence payload |

The existing resolver remained allocation-free and within benchmark noise after
the journal was added. The journal adds no default resolver or mutation-path
work; callers pay only when they construct it and append diagnostics.
