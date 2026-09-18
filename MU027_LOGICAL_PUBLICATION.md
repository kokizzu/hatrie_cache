# M-U27 Logical SQL Publications

`hat/hatSql.SQLPublication` is an opt-in logical change publication for SQL
consumers. It combines a fixed schema, contiguous revisions, bounded retained
history, replay from a consumer checkpoint, and a live subscription channel.
It is useful when a maintained SQL result needs to feed several downstream
consumers without treating the cache-state replication stream as the SQL
contract.

The publication is importable and in-memory. It does not start a server, alter
the default monitoring or replication lifecycle, persist history, or persist
checkpoints automatically. A connector owns durable checkpoint storage and
should acknowledge only after its output side effect is durable.

## Example

```go
publication, err := hatSql.NewSQLPublication(
	"orders",
	[]string{"id", "status"},
	hatSql.SQLPublicationOptions{},
)
if err != nil {
	return err
}

if err := publication.Append(hatSql.SQLPublicationBatch{
	Revision: 1,
	Frontier: 100,
	Deltas: []hatSql.SQLPublicationDelta{{
		Row:  hatSql.Row{"id": int64(7), "status": "paid"},
		Diff: 1,
	}},
}); err != nil {
	return err
}

subscription, err := publication.Subscribe(ctx, hatSql.SQLPublicationCheckpoint{})
if err != nil {
	return err
}
defer subscription.Close()

for batch := range subscription.Updates() {
	// Apply batch.Deltas to the downstream sink transaction.
	if err := subscription.Ack(hatSql.SQLPublicationCheckpoint{
		Revision: batch.Revision,
		Frontier: batch.Frontier,
	}); err != nil {
		return err
	}
}
return subscription.Err()
```

Delivered batches contain the fixed schema in `Columns` and signed row deltas
in `Deltas`:

| Revision | Frontier | Columns | Row | Diff |
|---:|---:|---|---|---:|
| 1 | 100 | `id,status` | `id=7,status=paid` | 1 |

The `Frontier` is a caller-owned logical progress value. It must never move
backward, while multiple revisions may share one frontier. `Progress` marks a
frontier-only batch; `Reset` tells a consumer to replace its current
multiset/state with the batch's supplied deltas; `Complete` is the terminal
batch for a finite publication.

## Delivery And Recovery

- The first append must have revision 1. Every later append must be exactly the
  previous revision plus one.
- A subscriber with a zero checkpoint replays the oldest retained history. A
  non-zero checkpoint receives only revisions after that checkpoint.
- If the requested non-zero checkpoint is older than retained history,
  `Subscribe` returns `ErrSQLPublicationCheckpointExpired`; the consumer must
  obtain a fresh snapshot and resume from a new checkpoint.
- `Ack` is monotone and cannot advance past a batch already delivered to that
  subscription. Persist the checkpoint only after the downstream transaction
  commits. Replaying after a crash is therefore at-least-once.
- `MarshalBinary` and `UnmarshalBinary` provide a fixed 24-byte `HPC1` record
  with a CRC32 checksum for a small caller-owned checkpoint file. The
  publication name is not embedded; checkpoints must be kept with their
  publication identity.
- A `Complete` batch closes the subscription normally after it is delivered.
  The terminal checkpoint can still be acknowledged. Calling `Close` on the
  publication ends active subscriptions with `ErrSQLPublicationClosed`.

## Backpressure And Bounds

An active subscription has a bounded pending buffer. When it is full, the
publisher does not block: that subscriber is closed with
`ErrSQLPublicationBackpressure`. The batch remains in retained history when
possible, so the consumer can resubscribe from its last acknowledged checkpoint
or request a fresh snapshot. The publisher returns the same backpressure error
after retaining the batch, making the dropped consumer visible to the caller.

Default limits are:

| Option | Default |
|---|---:|
| `MaxHistoryBatches` | 256 |
| `MaxBatchDeltas` | 4,096 |
| `MaxPendingBatches` | 64 |
| `MaxSubscribers` | 1,024 |
| `MaxColumns` | 256 |
| `MaxRowColumns` | 256 |

Zero selects a default. Negative or excessively large values are rejected.
Publication names, columns, and row keys must be valid UTF-8 without control
characters. Row keys must belong to the fixed schema, duplicate schema columns
are rejected, and zero differential values are rejected. The implementation
copies batch slices and row maps before retaining or delivering them, so callers
can safely reuse their input maps; nested mutable values should be treated as
immutable by the connector.

## Relationship To Existing APIs

`QueryDifferentialSubscription` already reports changes for one live query. It
owns query evaluation and subscription invalidation. `SQLPublication` is a
smaller transport boundary for already-produced logical SQL changes: it adds a
fixed publication schema, bounded replay history, consumer checkpoints, and
explicit slow-consumer behavior without coupling those responsibilities to the
query executor or cache-state replication.

Exactly-once connector transactions, durable history, fresh-snapshot generation,
and automatic publication wiring remain caller-owned. Those boundaries avoid
silently claiming stronger recovery guarantees than the downstream system can
provide.

## Verification

```sh
make test-mu027-logical-publication
make test-mu027-package
make race-mu027-logical-publication
make vet-mu027-logical-publication
```

The direct-channel control and publication measurements are recorded in
[BENCHMARK.md](BENCHMARK.md#mu-027-logical-sql-publications).
