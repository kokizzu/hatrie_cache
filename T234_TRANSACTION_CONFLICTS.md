# T234 Early Transaction Conflict Detection

`Space.BeginConflictDetectingTransaction()` provides an opt-in optimistic
write-conflict check for the T232 transaction API. A root commit returns
`ErrSpaceTransactionConflict` when another successful mutation changed one of
the transaction's staged keys after the transaction began.

```go
tx, err := space.BeginConflictDetectingTransaction()
if err != nil {
	return err
}
if err := tx.Put("account:42", []byte("new image")); err != nil {
	return err
}
if err := tx.Commit(); errors.Is(err, hatDataStructure.ErrSpaceTransactionConflict) {
	_ = tx.Rollback()
	// Reload and retry according to the application's policy.
}
```

The mode has deliberately narrow semantics:

- only keys in the final staged write set are checked;
- writes to disjoint keys can commit concurrently;
- nested transactions inherit conflict detection and are checked at the root
  commit;
- a rejected commit leaves the transaction open so the caller can inspect,
  revise, retry, or roll it back;
- ordinary `BeginTransaction` and `BeginMVCCTransaction` behavior is unchanged;
  ordinary transactions retain last-writer-wins semantics;
- the version map is allocated lazily on the first conflict-detecting
  transaction, and the per-space version state is process-local and not a
  replication or durable-restore conflict protocol.

The implementation assigns a monotonically increasing per-space generation to
successful mutations after conflict tracking is enabled and records the latest
generation per touched key. Vinyl's direct fast path is briefly quiesced when
tracking is first enabled, so a write already in flight cannot be missed.

## Measured Tradeoff

Commands:

```text
make benchmark-t234-before
make benchmark-t234
make benchmark-t234-quick
make benchmark-t234-direct
```

The existing transaction path keeps its allocation profile. The opt-in mode
adds one conflict-version map update and one extra transaction field allocation
in this workload. The rejected commit benchmark includes two transaction
begins, one successful commit, and one rejected commit.

| Workload | Median CPU | Memory | Relative CPU | Relative memory |
| --- | ---: | ---: | ---: | ---: |
| Regular Memtx transaction, before T234 | 532.1 ns/op | 736 B/op, 7 allocs/op | 1.00x | 1.00x |
| Regular Memtx transaction, after control | 562.7 ns/op | 736 B/op, 7 allocs/op | 1.06x | 1.00x |
| Conflict-detecting Memtx transaction | 743.0 ns/op | 800 B/op, 8 allocs/op | 1.32x vs after control | 1.09x vs after control |
| Regular Vinyl transaction, after control | 787.9 ns/op | 1,200 B/op, 9 allocs/op | 1.00x | 1.00x |
| Conflict-detecting Vinyl transaction | 1,085 ns/op | 1,264 B/op, 10 allocs/op | 1.38x | 1.05x |
| Rejected same-key conflict | 1,142 ns/op | 1,336 B/op, 12 allocs/op | separate workload | separate workload |

The feature is therefore opt-in: it improves correctness for competing
writers, not raw throughput. The default transaction API does not begin
conflict tracking and retains its prior behavior.

An atomic active-operation counter was tested as a replacement for the Vinyl
mutex gate. Its repeated control run reached about `61.1 ns/op` for default
Vinyl `Put`, versus about `54.7 ns/op` with the mutex gate, so that experiment
was rolled back. It is not part of the shipped implementation.

## Raw Benchmark Output

```text
Before, make benchmark-t234-before:
BenchmarkT234RegularTransactionWriteBaseline-32 2224416 532.1 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2323850 484.4 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2342467 523.8 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2171748 561.4 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2235478 555.2 ns/op 736 B/op 7 allocs/op

After, make benchmark-t234 (regular control and Memtx conflict path):
BenchmarkT234RegularTransactionWriteBaseline-32 2253980 601.0 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2119942 573.8 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2204472 562.7 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 1871233 553.8 ns/op 736 B/op 7 allocs/op
BenchmarkT234RegularTransactionWriteBaseline-32 2189617 555.0 ns/op 736 B/op 7 allocs/op
BenchmarkT234ConflictTransactionWrite-32 1807196 690.2 ns/op 800 B/op 8 allocs/op
BenchmarkT234ConflictTransactionWrite-32 1685619 746.0 ns/op 800 B/op 8 allocs/op
BenchmarkT234ConflictTransactionWrite-32 1497175 783.0 ns/op 800 B/op 8 allocs/op
BenchmarkT234ConflictTransactionWrite-32 1667044 708.9 ns/op 800 B/op 8 allocs/op
BenchmarkT234ConflictTransactionWrite-32 1466703 743.0 ns/op 800 B/op 8 allocs/op

After, make benchmark-t234-quick:
BenchmarkT234RegularTransactionWriteVinylBaseline-32 1476505 787.9 ns/op 1200 B/op 9 allocs/op
BenchmarkT234RegularTransactionWriteVinylBaseline-32 1452484 787.5 ns/op 1200 B/op 9 allocs/op
BenchmarkT234RegularTransactionWriteVinylBaseline-32 1543624 879.0 ns/op 1200 B/op 9 allocs/op
BenchmarkT234ConflictTransactionWriteVinyl-32 1000000 1043 ns/op 1264 B/op 10 allocs/op
BenchmarkT234ConflictTransactionWriteVinyl-32 970951 1085 ns/op 1264 B/op 10 allocs/op
BenchmarkT234ConflictTransactionWriteVinyl-32 1148029 1087 ns/op 1264 B/op 10 allocs/op
BenchmarkT234ConflictTransactionRejected-32 1015650 1143 ns/op 1336 B/op 12 allocs/op
BenchmarkT234ConflictTransactionRejected-32 1000000 1109 ns/op 1336 B/op 12 allocs/op
BenchmarkT234ConflictTransactionRejected-32 926556 1142 ns/op 1336 B/op 12 allocs/op

Rejected atomic-gate trial, default Vinyl Put:
BenchmarkT234DirectVinylPutDefault-32 20167036 59.36 ns/op 8 B/op 1 allocs/op
BenchmarkT234DirectVinylPutDefault-32 20986054 65.04 ns/op 8 B/op 1 allocs/op
```
