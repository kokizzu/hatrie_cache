# T233 MVCC Transactions

`hatDataStructure.Space.BeginMVCCTransaction` creates an opt-in repeatable
read view for a `SpaceTransaction`. The view is captured once at transaction
start, remains stable across ordinary Space writes, and is inherited by nested
transactions.

## API

```go
tx, err := space.BeginMVCCTransaction()
if err != nil {
	return err
}
defer tx.Rollback()

for moreWork() {
	value, ok := tx.Get("account/1")
	if ok {
		process(value)
	}
	if err := tx.Yield(ctx); err != nil {
		return err
	}
}

return tx.Commit()
```

`BeginTransaction` remains the live-read T232 API. Use
`BeginMVCCTransaction` when repeated reads must observe one logical point in
time. `SpaceTransaction.Get` checks staged writes first, then the inherited
MVCC view. `Put`, `Delete`, nested scopes, and root commit keep the T232
semantics.

`Yield(ctx)` checks cancellation, calls the Go scheduler, and checks
cancellation again. It does not hold a Space or engine lock while the caller
is between transaction methods, so other writers can make progress. A yield
does not commit, rollback, or discard the read view.

## Snapshot Representation

The snapshot copies only the memtx map or Vinyl memtable/run lists. Existing
value byte slices and immutable Vinyl runs are shared, so the feature does not
copy every value at transaction start. Later writes replace map entries or
append new LSM records; they do not mutate the captured entries.

The snapshot is process-local and read-only. It is not a durable checkpoint,
does not provide crash recovery, and does not detect write conflicts. Root
commit still evaluates the current Space state and uses T232 last-writer-wins
behavior. T234 is reserved for early conflict detection.

## Measured Tradeoff

Commands:

```text
make benchmark-t233-before
make benchmark-t233
```

The ordinary eight-key transaction read control retained 112 B/op and 9
allocs/op. A same-tree control rerun measured 364.1 ns/op, matching the
pre-T233 median of 363.5 ns/op within benchmark noise. The opt-in MVCC read
view measured 828.3 ns/op, 592 B/op, and 13 allocs/op for eight keys. The
additional snapshot metadata is the cost of repeatable reads; ordinary
transactions and ordinary Space writes do not pay it.

| Workload | Median CPU | Memory | Relative CPU | Relative memory |
| --- | ---: | ---: | ---: | ---: |
| Regular eight-key transaction read, before T233 | 363.5 ns/op | 112 B/op, 9 allocs/op | 1.00x | 1.00x |
| Regular eight-key transaction read, same-tree control | 364.1 ns/op | 112 B/op, 9 allocs/op | 1.00x | 1.00x |
| MVCC eight-key transaction read | 828.3 ns/op | 592 B/op, 13 allocs/op | 2.28x | 5.29x |
| MVCC begin plus one cooperative yield | 448.2 ns/op | 176 B/op, 4 allocs/op | workload differs | workload differs |

This tradeoff is intentional and bounded to callers that request MVCC. The
default transaction and write paths keep their prior allocation profile.

## Raw Benchmark Output

```text
Before, make benchmark-t233-before:
BenchmarkT233RegularTransactionReadBaseline-32 3159680 366.5 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3294040 363.5 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3378792 373.7 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3397534 354.7 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3181076 345.9 ns/op 112 B/op 9 allocs/op

After, make benchmark-t233:
BenchmarkT233RegularTransactionReadBaseline-32 3215751 392.7 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 2881831 402.4 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 2950935 398.8 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 2968538 400.6 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3199609 394.1 ns/op 112 B/op 9 allocs/op
BenchmarkT233MVCCTransactionRead-32 1000000 1032 ns/op 592 B/op 13 allocs/op
BenchmarkT233MVCCTransactionRead-32 1000000 1025 ns/op 592 B/op 13 allocs/op
BenchmarkT233MVCCTransactionRead-32 1338994 813.7 ns/op 592 B/op 13 allocs/op
BenchmarkT233MVCCTransactionRead-32 1413614 828.3 ns/op 592 B/op 13 allocs/op
BenchmarkT233MVCCTransactionRead-32 1523499 807.7 ns/op 592 B/op 13 allocs/op
BenchmarkT233MVCCTransactionYield-32 2714761 432.1 ns/op 176 B/op 4 allocs/op
BenchmarkT233MVCCTransactionYield-32 2597443 448.2 ns/op 176 B/op 4 allocs/op
BenchmarkT233MVCCTransactionYield-32 2719776 454.6 ns/op 176 B/op 4 allocs/op
BenchmarkT233MVCCTransactionYield-32 2616337 464.4 ns/op 176 B/op 4 allocs/op
BenchmarkT233MVCCTransactionYield-32 2552239 432.2 ns/op 176 B/op 4 allocs/op

Same-tree control rerun, make benchmark-t233-before:
BenchmarkT233RegularTransactionReadBaseline-32 3173666 370.0 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3428236 364.1 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3374816 397.6 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3425546 327.7 ns/op 112 B/op 9 allocs/op
BenchmarkT233RegularTransactionReadBaseline-32 3311298 353.4 ns/op 112 B/op 9 allocs/op
```
