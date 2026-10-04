# Conflict Decision Log

`hatReplication.ConflictDecisionLog` is an opt-in bounded stream for
diagnosing replicated write conflicts. It records the source space, the
deterministic winner and loser versions, and a 16-byte key digest. It never
stores the original key or either write value.

## Usage

```go
log, err := hatReplication.NewConflictDecisionLog(4096)
event, err := log.Record(
	"orders",
	"customer/42",
	hatReplication.ConflictVersion{Timestamp: 10, NodeID: "node-a", Sequence: 7},
	hatReplication.ConflictVersion{Timestamp: 11, NodeID: "node-b", Sequence: 3},
)
tail, err := log.Tail(lastSequence, 256)
snapshot, err := log.Snapshot()
restored, err := hatReplication.RestoreConflictDecisionLog(snapshot)
```

`Record` uses the same ordering as `ResolveConflictVersion`: timestamp,
`NodeID`, then per-node sequence. Equal versions are rejected because they are
not a conflict. The log is safe for concurrent `Record`, `Tail`, and
`Snapshot` calls.

The caller owns persistence and integration with the replication apply path.
For durable use, write `Snapshot()` with the application's atomic file/object
publication protocol and restore it before accepting new conflict events.
There is no default global log and no change to the normal conflict resolver.

## Retention And Cursors

The log is a fixed-capacity ring. When it fills, the oldest event is dropped
and `Dropped` increases. `Tail(after, limit)` returns events strictly newer
than `after`. A zero cursor starts at the oldest retained event; a nonzero
cursor older than the retained range returns `ErrConflictDecisionLogHistoryGap`
so a consumer cannot silently miss decisions.

Current bounds are 65,536 events, 256-byte space and node labels, 4 KiB input
keys, 4,096 events per tail, and 16 MiB per snapshot. A zero-value log is
rejected rather than implicitly allocating an unbounded ring.

## Redaction And Security

The key digest is the first 16 bytes of SHA-256. It prevents the original key
from appearing in the log or snapshot and is suitable for correlating the same
key across events. It is not encryption: low-entropy keys can still be
dictionary-tested. Do not treat the digest as a secret or authorization
credential. Values are never accepted by this API.

Snapshots use deterministic `HCD1` framing with canonical varints, fixed-size
version timestamps, length-bounded strings, and CRC32C Castagnoli. Restore
rejects unknown versions, malformed lengths, invalid version pairs, counter
inconsistencies, trailing bytes, and checksum failures before publishing any
state.

## Benchmark

The conflict resolver baseline and the opt-in record path were measured on
Linux/amd64, AMD Ryzen 9 5950X, five repetitions. The record workload hashes
one 12-byte key and writes one event to a 1,024-entry ring. Snapshot measures
serializing 128 retained events.

| Operation | Existing baseline | Opt-in log | Tradeoff |
| --- | ---: | ---: | ---: |
| Winner selection/record | 2.15 ns/op, 0 B/op, 0 allocs | 100.7 ns/op, 0 B/op, 0 allocs | 46.8x CPU overhead on the conflict path; no default-path cost |
| Snapshot of 128 events | N/A | 9.95 us/op, 32,952 B/op, 14 allocs | 7,187-byte durable snapshot; caller chooses frequency |

The log is therefore an observability and recovery feature, not a faster
conflict resolver. Its cost is paid only when a caller explicitly records a
decision.
