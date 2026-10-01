# T-U38 Conflict Introspection Stream

`hatReplication.ConflictEventLog` is an opt-in, bounded conflict history for
callers that already resolve replicated writes. It does not hook into conflict
resolution automatically, so the normal write path has no new branch,
allocation, lock, or disk cost.

## Security model

`ConflictEventInput.Key` is never retained. The log stores an HMAC-SHA256
fingerprint using the caller-provided `Secret`, which must be at least 16 bytes
and should come from a secret manager. This prevents a reader of the event file
from recovering low-entropy keys with an unsalted hash. Keep the same secret
after restart when fingerprints must be correlated across files; the secret is
never written to the file.

The event retains the named space, both conflict versions, the selected source,
and the decision (`left_wins`, `right_wins`, `equal`, or `rejected`). It does
not retain tuple values, raw keys, or error text.

## Usage

```go
log, err := hatReplication.OpenConflictEventLog(
    "state/conflicts.hce",
    hatReplication.ConflictEventLogOptions{
        Capacity: 1024,
        Secret:   conflictEventSecret,
    },
)
if err != nil {
    return err
}
defer log.Close()

event, err := log.Record(hatReplication.ConflictEventInput{
    Space:    "orders",
    Key:      orderKeyBytes,
    Left:     leftVersion,
    Right:    rightVersion,
    Decision: hatReplication.ConflictEventDecisionRightWins,
    Winner:   &rightVersion,
})
if err != nil {
    return err
}

page, err := log.ReadAfter(cursor, 128)
if err != nil {
    return err
}
nextCursor := page.NextSequence
_ = event
_ = nextCursor
```

`ReadAfter` returns a bounded page and reports `ErrConflictEventHistoryGap`
when retention has already removed the requested cursor. `WaitAfter` uses a
context and a close-and-replace notification channel, so idle consumers do not
create one goroutine each and producers never block on a slow consumer. The
caller provides backpressure by choosing the page limit and advancing its
cursor.

## Durable format

An opened path uses a compact HCE1 binary file. Each record has a bounded frame
and CRC32C validation. Appends are written and synced before the event becomes
visible. When the in-memory ring is full, the retained events are rewritten to
a temporary file, synced, and atomically renamed; this keeps disk usage bounded
at the cost of a retention-rollover rewrite. A corrupt frame or capacity
mismatch is rejected during open.

The default capacity is 1,024 events and the maximum is 65,536. In-memory logs
are appropriate for short-lived diagnostics; use a durable path when replay
after restart matters.

## Measured tradeoff

On the benchmark host (AMD Ryzen 9 5950X), three 200 ms runs measured:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing conflict resolution | 3.68 | 0 | 0 |
| Resolution plus in-memory event record | 768.6 | 832 | 12 |
| One-event replay page | 194.3 | 288 | 4 |
| Durable append with `fsync` | 681,743 | 2,172 | 16 |

The in-memory record path is about 209x the CPU cost of the bare resolver, and
the steady-state durable path is about 185,500x. Those costs are paid only when
the caller explicitly records conflict events. The benchmark does not hide the
retention-rollover rewrite; it measures steady-state appends before the 4,096
event capacity is reached.
