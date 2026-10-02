# T-U20 Online Tuple Upgrade

`hatDataStructure.OnlineTupleUpgrade` coordinates an online transition from
one `VersionedTuple` format to a newer format while callers continue to read
and write. It is an explicit coordinator; it does not change existing tuple
reads, writes, storage, or defaults.

## Lifecycle

1. Construct a `TupleMigrationPlan` and an `OnlineTupleUpgrade` with the
   source and target format versions.
2. Call `Begin` and keep serving reads and writes through the coordinator.
3. Run bounded `MigrateBatch` calls until the scan reaches the end. Source
   rows are migrated and written back; target rows are skipped.
4. When the scan completes, the state becomes `Ready`.
5. Call `Cutover`. New source-format reads and writes are rejected after this
   point; target-format operations continue.

`Read` performs read repair for source rows while the upgrade is running or
ready. `Write` accepts source rows during that window and normalizes them to
the target format. A failed scan can be resumed with `Resume`; cancellation
does not mark the upgrade failed.

## Storage contract

The caller supplies an `OnlineTupleUpgradeStore` with `Get`, `Put`, and
`Scan`. The store owns transactionality, snapshot isolation, key ordering,
and synchronization between a scan and concurrent writes. `Scan` must stop
when its callback returns `ErrOnlineTupleUpgradeBatchComplete` and must
propagate callback errors. A production store should make each read-repair
or migration `Put` conditional on the version observed by `Get` when lost
updates are possible.

The coordinator serializes migration batches and protects its lifecycle and
statistics. It does not provide a distributed lock or durable checkpoint.
Persist the state and resume policy in the owning storage/service if a
process restart must continue an upgrade.

## Safety behavior

- Invalid lifecycle transitions return `ErrOnlineTupleUpgradeState`.
- Rows with versions other than source or target fail the operation and move
  the coordinator to `Failed`.
- A completed scan is required before `Cutover`.
- A failed upgrade can be resumed after the caller repairs the underlying
  issue.
- Batch limits bound migration work and avoid one uninterruptible scan.
- Context cancellation preserves `Running`, so a later batch can resume.

## Verification

Focused tests cover dual reads, read repair, source-write normalization,
bounded batches, cutover rejection, cancellation/resume, mixed-version
failure, invalid transitions, and independent statistics snapshots:

```sh
make round27-online-focused
make round27-online-race-focused
make round27-online-baseline-test
make round27-online-vet
```

The coordinator is opt-in and should be introduced behind an application
feature flag. Keep the old write path available until the target format has
been verified and a rollback decision has been made. A rollback before
cutover is to stop the coordinator and continue using the source format; a
rollback after cutover requires a reverse migration plan and a new controlled
upgrade.

## Raw benchmark samples

The benchmark target uses `-benchtime=1s -count=5 -benchmem`:

| Workload | Samples (ns/op) | B/op | Allocs/op |
|---|---|---:|---:|
| Baseline manual two-format packing | 437.0, 435.5, 433.1, 435.7, 434.2 | 392 | 3 |
| Adoption manual two-format packing | 430.2, 427.6, 437.6, 456.4, 447.0 | 392 | 3 |
| Adoption coordinator write normalization | 598.1, 589.8, 605.3, 584.3, 591.6 | 464 | 7 |

The median is used in `BENCHMARK.md`. The coordinator comparison is a
feature-cost measurement, not a claim that online coordination is faster than
direct migration.
