# T232 Space Transactions

`hatDataStructure.Space.BeginTransaction` provides an opt-in atomic write
scope for both memtx and Vinyl spaces. A transaction stages copied values and
does not change the Space until the root transaction commits.

## API

```go
tx, err := space.BeginTransaction()
if err != nil {
	return err
}
defer tx.Rollback()

if err := tx.Put("account/1", []byte("active")); err != nil {
	return err
}

nested, err := tx.BeginNested()
if err != nil {
	return err
}
if err := nested.Put("audit/1", []byte("created")); err != nil {
	return err
}
if err := nested.Commit(); err != nil {
	return err
}

return tx.Commit()
```

`SpaceTransaction.Get` reads the transaction view: its own staged changes,
then committed parent changes, then the current Space state. Values are copied
on both `Put` and `Get`.

`BeginNested` creates a rollback boundary. A nested `Commit` merges its final
changes into the parent without touching storage. A nested `Rollback` discards
only that child. A parent cannot commit or roll back while a child is open;
close children first. `Rollback` is explicit and returns an error when the
scope is already closed or still has an open child.

## Atomicity

The root commit:

1. Resolves each key to its final staged mutation and sorts keys for stable
   callback order.
2. Validates value limits, memtx capacity, and all `BeforeReplace` callbacks
   before changing storage.
3. Applies the complete batch while holding the existing Space lock. Vinyl
   also holds its LSM lock, so direct Vinyl readers and writers cannot observe
   an intermediate batch state.
4. Emits `OnReplace` and `AfterReplace` only after storage accepts the whole
   batch.

If validation fails, no storage mutation or success callback is emitted. If a
Vinyl engine operation fails after starting, its in-memory memtable, runs, and
compaction counters are restored from a pre-commit snapshot. This is logical
rollback for the current process, not a replacement for a durable WAL or a
crash-recovery protocol.

The existing `AfterReplace` transaction IDs remain per successful replacement,
including replacements from one root commit. They are not a new transaction-
wide ID. IDs reserved before a later engine failure can leave a gap, matching
the existing T231 contract.

Callbacks run while the Space mutation is serialized and must not re-enter the
same Space. Callback side effects are outside storage rollback; callbacks
should not panic or assume that they can be undone if a later external action
fails.

Transactions currently use last-writer-wins behavior when another caller
changes a key between staging and commit. MVCC snapshots and early conflict
detection are separate planned features (T233/T234).

## Measured Tradeoff

Benchmarks ran with:

```text
make benchmark-t232-before
make benchmark-t232
```

The default path stayed within normal benchmark noise: `Space.Put` moved from
47.25 ns/op, 8 B/op, 1 alloc/op before T232 to 47.08 ns/op, 8 B/op, 1
alloc/op after T232. The explicit transaction path is intentionally more
expensive because it provides isolation from partial writes:

| Workload | Before T232 | After T232 | Relative CPU | Relative memory |
| --- | ---: | ---: | ---: | ---: |
| One ordinary `Space.Put` | 47.25 ns, 8 B, 1 alloc | 47.08 ns, 8 B, 1 alloc | 1.00x | 1.00x |
| Eight ordinary sequential puts | 417.1 ns, 64 B, 8 allocs | 406.6 ns, 64 B, 8 allocs | 0.97x | 1.00x |
| One-key transaction | not available | 545.0 ns, 736 B, 7 allocs | 11.53x vs one put | 92.00x vs one put |
| Eight-key transaction | not available | 2,458 ns, 2,896 B, 36 allocs | 5.89x vs eight puts | 45.25x vs eight puts |

The transaction cost is therefore opt-in and should be reserved for operations
that need atomic multi-key visibility or nested rollback. Ordinary point writes
retain the existing default path.

## Raw Benchmark Output

```text
Before, make benchmark-t232-before:
BenchmarkT232SpacePutBaseline-32            24843915  47.39 ns/op   8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            26552014  47.05 ns/op   8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            26817489  46.32 ns/op   8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            25305820  47.25 ns/op   8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            24341313  47.84 ns/op   8 B/op  1 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2850096 417.1 ns/op  64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2881417 399.5 ns/op  64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     3010238 424.7 ns/op  64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     3015601 425.6 ns/op  64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2770264 414.5 ns/op  64 B/op  8 allocs/op

After, make benchmark-t232:
BenchmarkT232SpacePutBaseline-32            25037659  47.52 ns/op     8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            26047956  46.75 ns/op     8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            23369410  47.04 ns/op     8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            26720824  47.08 ns/op     8 B/op  1 allocs/op
BenchmarkT232SpacePutBaseline-32            28278007  49.51 ns/op     8 B/op  1 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2810211  406.6 ns/op    64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2927337  421.9 ns/op    64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     3088609  401.6 ns/op    64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2633832  404.6 ns/op    64 B/op  8 allocs/op
BenchmarkT232SequentialBatchBaseline-32     2762337  423.0 ns/op    64 B/op  8 allocs/op
BenchmarkT232SpaceTransaction-32             2121204  541.8 ns/op   736 B/op  7 allocs/op
BenchmarkT232SpaceTransaction-32             2209981  545.0 ns/op   736 B/op  7 allocs/op
BenchmarkT232SpaceTransaction-32             2193385  549.0 ns/op   736 B/op  7 allocs/op
BenchmarkT232SpaceTransaction-32             2236368  556.1 ns/op   736 B/op  7 allocs/op
BenchmarkT232SpaceTransaction-32             2325452  527.8 ns/op   736 B/op  7 allocs/op
BenchmarkT232TransactionalBatch-32            539709 2379 ns/op   2896 B/op 36 allocs/op
BenchmarkT232TransactionalBatch-32            492825 2484 ns/op   2896 B/op 36 allocs/op
BenchmarkT232TransactionalBatch-32            417927 2424 ns/op   2896 B/op 36 allocs/op
BenchmarkT232TransactionalBatch-32            435266 2509 ns/op   2896 B/op 36 allocs/op
BenchmarkT232TransactionalBatch-32            534766 2458 ns/op   2896 B/op 36 allocs/op
```
