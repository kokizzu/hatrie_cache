# M-U18 Exactly-Once Sink Checkpoints

M-U18 adds an importable `hatSql.SQLSinkExactlyOnceLedger` for external sink
delivery. It combines three pieces that were previously separate:

- an explicit `SQLSinkCommit.IdempotencyKey` passed to the external callback;
- one monotone acknowledged frontier per sink partition; and
- bounded retained commit history with snapshot/restore support.

The default capacity is `1024` retained commits and the maximum is `65536`.
Frontiers are retained independently, so history eviction does not move a
sink backwards. The ledger is in-memory unless `CheckpointStore` is configured.

## Usage

```go
store, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore("/var/lib/app/sink-checkpoints.json")
if err != nil {
	return err
}
ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(ctx, hatSql.SQLSinkExactlyOnceOptions{
	Name:             "warehouse",
	Capacity:         1024,
	CheckpointStore:  store,
})
if err != nil {
	return err
}

commit := hatSql.SQLSinkCommit{
	Sink:           "warehouse",
	TransactionID:  "orders-txn-42",
	IdempotencyKey: "orders/partition-0/42",
	Progress: []hatSql.SQLSinkProgress{
		{Sink: "warehouse", Partition: "0", Frontier: 42},
	},
}
committed, err := ledger.CommitContext(ctx, commit, func(key string) error {
	// The sink must send key with the output and deduplicate it atomically.
	return warehouse.Put(ctx, key, payload)
})
```

`committed` is true only after the callback succeeds and, when configured, the
checkpoint store durably saves the new state. A replay of the same key and
commit definition returns false without invoking the callback. Reusing a key
or transaction ID with a different definition returns
`ErrSQLSinkExactlyOnceConflict`. A non-new frontier returns
`ErrSQLSinkExactlyOnceStale`; another commit for the same partition while one
is delivering returns `ErrSQLSinkExactlyOnceInFlight`.

For compatibility, an empty `IdempotencyKey` uses `TransactionID`. New code
should set the key explicitly and send it to the external sink.

## Durability Boundary

The file store writes a compact versioned `HSE1` binary checkpoint with private
`0600` permissions, uses same-directory temporary-file replacement, syncs the
file and directory, and rejects symlink, non-regular, or group/world-readable
checkpoint files. It reads legacy JSON checkpoints and migrates them to binary
on the next successful save. `Snapshot` and `Restore` are also available for
applications that use another durable store.

This is an exactly-once retry contract, not a claim that a library can make an
arbitrary remote side effect transactional. A crash after the external sink
accepts output but before the checkpoint save can cause a retry. The external
sink must therefore treat the supplied idempotency key as a durable unique key
or provide its own transaction that atomically stores the output and key.

The retained history is deliberately bounded. After a key is evicted, the
frontier still rejects an older commit, but a new commit with a newer frontier
may use a new key. Choose capacity to cover the maximum retry/recovery window.

## Measurements

Machine: AMD Ryzen 9 5950X, Linux/amd64, `GOMAXPROCS=1`. The pre-change
benchmark uses the existing unbounded in-memory `SQLSinkCommitCoordinator`; the
M-U18 path uses the default bounded ledger. Five `500ms` samples were run for
the in-memory comparison. Durable samples used three `100ms` runs and a
`1024`-commit capacity.

| Path | Median ns/op | B/op | allocs/op | Improvement / cost |
| --- | ---: | ---: | ---: | --- |
| Existing coordinator, new commit | 1,288 | 513 | 7 | baseline |
| M-U18 ledger, new commit | 943.9 | 482 | 9 | `1.36x` faster, `1.06x` lower bytes, +2 allocs |
| M-U18 ledger, duplicate replay | 203.6 | 72 | 2 | fast no-callback retry path |
| M-U18 ledger + in-memory checkpoint store | 56,468 | 82,492 | 12 | `59.8x` slower than in-memory commit; durable-state assembly |
| M-U18 ledger + file checkpoint store | 3,768,925 | 1,108,798 | 5,210 | `3,993x` slower than in-memory commit; `fsync` and full binary rewrite |

The ledger’s default in-memory path adds bounded-history and token checks while
using less measured memory than the old unbounded map in this workload. The
durable store is opt-in and intentionally pays a large persistence cost for a
checkpoint that survives process loss; it should normally be used at a sink
batch boundary, not for each individual row.

Benchmark targets:

```text
make benchmark-mu018-baseline
make benchmark-mu018-exactly-once-sink
make benchmark-mu018-durable-sink
```

Correctness targets:

```text
make test-mu018-exactly-once-sink
make test-mu018-package
make race-mu018-exactly-once-sink
make vet-mu018-exactly-once-sink
```
