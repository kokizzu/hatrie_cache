# M-U19 Source Transaction Envelopes

`SQLSourceTransactionEnvelope` provides one source-adapter contract for a
transaction that changes multiple named relations. It is inspired by
Materialize's source transaction and commit-marker model, while keeping the
payload/application policy in the caller.

## Usage

```go
coordinator := hatSql.NewSQLSourceTransactionEnvelopeCoordinator()
envelope := hatSql.SQLSourceTransactionEnvelope{
    Source: "orders-cdc",
    Transaction: hatSql.SQLSourceTransaction{
        ID: "tx-1042",
        Offsets: []hatSql.SQLSourceOffset{
            {Source: "orders-cdc", Partition: "0", Offset: 812},
            {Source: "orders-cdc", Partition: "1", Offset: 447},
        },
    },
    Relations: []string{"orders", "customers"},
}

committed, err := coordinator.IngestEnvelope(envelope, func() error {
    // Stage both relation changes in one transaction, then commit them.
    return applyAllRelationsAtomically(envelope)
})
```

The callback runs once for a new envelope. Concurrent duplicates wait for the
first callback, and successful duplicates return `false, nil`. A commit marker
is retained only after the callback returns `nil`; callback errors remove the
in-flight entry so the source can retry it. The coordinator does not retain
row payloads and cannot roll back an arbitrary external system. The callback
must therefore use the destination's transaction/staging API and must make
the external commit idempotent across a crash after external commit but before
the coordinator checkpoint.

## Validation And Recovery

`IngestEnvelope` requires a source, transaction ID, at least one source offset,
and at least one relation. Source and partition names are trimmed; offsets and
relation names are canonicalized into deterministic order. Duplicate offsets,
duplicate relations, source mismatches, NUL-containing names, and oversized
relation metadata are rejected before coordinator state changes. The relation
set participates in conflict detection, so reusing a transaction ID with a
different relation set is not silently treated as a duplicate.

Use the envelope snapshot methods for recovery so relation membership is not
lost:

```go
checkpoint := coordinator.SnapshotEnvelopes()
recovered := hatSql.NewSQLSourceTransactionEnvelopeCoordinator()
if err := recovered.RestoreEnvelopes(checkpoint); err != nil {
    return err
}
```

Snapshots are independently owned and deterministic. Invalid or duplicate
restore input leaves the existing coordinator unchanged. The older
`Ingest`, `Snapshot`, and `Restore` methods remain available for source
adapters that do not expose relation metadata; their snapshots intentionally
use the legacy relation-free format.

## Measured Cost

The reproducible command is:

```text
make benchmark-mu019-source-transaction-envelope
```

The target uses `GOMAXPROCS=1`, a 500 ms window, five samples, two source
partitions, and a 256-transaction coordinator window. The reported value is
the median sample from the pinned run below.

| Path | ns/op | B/op | allocs/op | Relative latency | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Existing `Ingest` before envelope support | 649.4 | 649 | 8 | 1.00x | 1.00x |
| Two-relation `IngestEnvelope` | 717.9 | 713 | 10 | 1.11x slower | 1.10x |

The two extra allocations are bounded relation metadata/state work. The old
relation-free API keeps its original eight allocations in the paired run. The
tradeoff buys cross-relation identity, single-flight commit markers, and
relation-preserving recovery without retaining row data.
