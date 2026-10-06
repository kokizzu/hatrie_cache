# Conflict Introspection

`hatReplication.ConflictEventLog` is an opt-in bounded diagnostic log for
replication conflict decisions. It is deliberately separate from
`ResolveConflictVersion` and `ConflictPolicyRegistry`, so ordinary conflict
resolution has no new lock, allocation, or branch.

## Security and privacy

- Store only a caller-computed `[16]byte` key digest. The log has no API that
  accepts or retains the original key bytes.
- Use a keyed digest or HMAC when keys are guessable and digest disclosure
  would reveal sensitive information.
- Space names and node IDs are retained in events and snapshots. Protect
  snapshots with the same access controls as replication diagnostics.
- The ring is bounded. A cursor older than the retained window returns
  `ErrConflictEventCursorStale` rather than silently skipping history.

## Usage

```go
log, err := hatReplication.NewConflictEventLog(4096)
if err != nil {
    return err
}

// Compute this from the key without passing the key to the log.
sum := sha256.Sum256(key)
var digest [16]byte
copy(digest[:], sum[:16])

left := hatReplication.ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 7}
right := hatReplication.ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 3}
winner, err := hatReplication.ResolveConflictVersion(left, right)
if err != nil {
    return err
}
decision := hatReplication.ConflictEventDecisionLeft
if winner == right {
    decision = hatReplication.ConflictEventDecisionRight
}
_, err = log.Append(hatReplication.ConflictEvent{
    Space:     "orders",
    KeyDigest: digest,
    Left:      left,
    Right:     right,
    Winner:    winner,
    Decision:  decision,
    Policy:    hatReplication.ConflictPolicyLastWriteWins,
})
if err != nil {
    return err
}

events, next, err := log.Read(0, 100)
_ = events
_ = next
```

`Read(after, limit)` returns events strictly after `after` in sequence order.
Pass `0` for the initial cursor and `0` for the default bounded batch size.
`Snapshot` returns a detached, JSON-compatible value. Persist that value with
the application's normal durable snapshot mechanism and restore it with
`NewConflictEventLogFromSnapshot`.

Rejected policy decisions are represented by
`ConflictEventDecisionRejected` and have a zero `Winner`. The caller records a
rejection after the policy resolver returns `ErrConflictRejected`.

## Measurement

The focused benchmark compares the existing resolver with the diagnostic
append path on the benchmark host:

| Path | Result |
| --- | ---: |
| Direct `ResolveConflictVersion` control | 2.6-2.9 ns/op, 0 B/op, 0 allocs/op |
| `ConflictEventLog.Append` | 38-67 ns/op, 0 B/op, 0 allocs/op |

This is an observability tradeoff, not a hot-path optimization. The feature is
not wired into the default resolver, has fixed event capacity, and only pays
the append cost when a caller explicitly enables and records diagnostics.
