# T-U20 Online Space Upgrade

`hatDataStructure.OnlineTupleUpgrade` provides an opt-in coordinator for
converting versioned tuples while reads and writes continue. It builds on the
existing immutable `TupleFormat` and `VersionedTuple` validation boundaries;
it does not replace or alter the direct tuple APIs.

## Lifecycle

1. Create a plan with the previous format, next format, total row count, and
   forward/reverse conversion callbacks.
2. During `active`, `Read` accepts either version and converts old rows to the
   next format. `Write` emits the next format. `MigrateBatch` converts a
   bounded caller-supplied window and atomically advances `NextRow` only after
   every row in the window succeeds.
3. Persist `Snapshot()` after the caller persists each converted batch. A
   `TupleUpgradeCheckpoint` can be encoded as a bounded checksummed HUP1
   record and restored after restart.
4. `BeginCutover` is accepted only after `NextRow >= TotalRows`. During
   `cutover`, both formats remain readable while the caller performs its
   publication step. `CompleteCutover` makes the next format the only accepted
   read format.
5. `AbortCutover` returns to active migration. `Rollback` makes the previous
   format readable/writable and uses the reverse converter for rows already
   upgraded. A completed upgrade is terminal; a new forward attempt should use
   a new plan.

The coordinator does not own table storage, WAL ordering, row locks, or the
checkpoint file. The caller must persist the converted batch and its matching
checkpoint atomically enough for its storage model, then retry from the
checkpoint after a crash.

## Example

```go
upgrade, err := hatDataStructure.NewOnlineTupleUpgrade(
    "users-v1-v2", previousFormat, nextFormat, totalRows,
    hatDataStructure.TupleUpgradeConverter{
        ToNext:     convertToNext,
        ToPrevious: convertToPrevious,
    },
)

converted, err := upgrade.MigrateBatch(ctx, rows, 256)
checkpoint := upgrade.Snapshot()
wire, err := hatDataStructure.MarshalTupleUpgradeCheckpoint(checkpoint)
// Persist converted and wire using the storage layer's transaction boundary.
```

Batch conversion is optimistic: callbacks run without the coordinator lock,
and the final commit checks phase, generation, and starting row. A concurrent
batch therefore returns `ErrTupleUpgradeConflict` instead of overwriting
progress. Conversion errors and context cancellation return without advancing
any progress.

## HUP1 checkpoint

The checkpoint contains the plan ID, previous/next format versions, total and
next row counters, migrated count, generation, and lifecycle phase. It is
bounded to 256 bytes, limits IDs to 128 bytes, rejects semantic inconsistencies,
rejects trailing bytes, and uses CRC32C Castagnoli. It is a progress fence, not
a replacement for the row data or a WAL.

## Benchmark

The matched benchmark ran through `make codex-tu20-next-bench` five times per
benchmark on Linux amd64, AMD Ryzen 9 5950X. Existing direct rows are the
pre-upgrade baseline and use the same three-field fixture.

| Path | Median ns/op | B/op | allocs/op | Relative to direct analogue |
| --- | ---: | ---: | ---: | ---: |
| Direct tuple validate | 46.96 | 0 | 0 | 1.00x |
| Upgrade read, already-next row | 93.81 | 0 | 0 | 2.00x |
| Direct tuple marshal | 98.94 | 80 | 1 | 1.00x |
| Direct tuple unmarshal | 85.34 | 40 | 3 | 1.00x |
| Upgrade read, old row conversion | 714.4 | 853 | 5 | 7.22x vs validate |
| HUP1 checkpoint marshal | 52.71 | 80 | 1 | New operation |
| HUP1 checkpoint unmarshal | 51.36 | 16 | 1 | New operation |

The coordinator is intentionally opt-in. Already-next reads add a lock and
phase/version check, while old-row conversion pays for unpack, callback, and
repack. Existing direct paths retain their zero-allocation or one-allocation
behavior. Reusing one CRC32C table reduced checkpoint marshal from 54.16 ns to
52.71 ns and unmarshal from 56.96 ns to 51.36 ns across separate five-run
measurements; wire size stayed 70 bytes and allocation counts stayed constant.

### Raw matched runs

```text
BenchmarkTU20DirectVersionedTupleValidate: 47.27 45.89 46.24 47.15 46.96 ns/op; 0 B/op; 0 alloc/op
BenchmarkTU20DirectVersionedTupleMarshal: 98.87 98.19 98.94 100.4 99.10 ns/op; 80 B/op; 1 alloc/op
BenchmarkTU20DirectVersionedTupleUnmarshal: 84.97 84.99 85.34 87.12 86.71 ns/op; 40 B/op; 3 alloc/op
BenchmarkTU20UpgradeReadOld: 727.8 719.4 714.4 710.3 713.1 ns/op; 853 B/op; 5 alloc/op
BenchmarkTU20UpgradeReadNext: 92.77 93.45 95.06 93.81 94.26 ns/op; 0 B/op; 0 alloc/op
BenchmarkTU20UpgradeCheckpointMarshal: 52.68 53.12 52.71 52.83 52.59 ns/op; 80 B/op; 1 alloc/op; 70 wire bytes/op
BenchmarkTU20UpgradeCheckpointUnmarshal: 51.36 51.54 51.65 51.14 51.03 ns/op; 16 B/op; 1 alloc/op; 70 wire bytes/op
```
