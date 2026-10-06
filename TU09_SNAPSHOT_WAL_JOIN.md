# Snapshot + Journal Join

`JoinFromSnapshotAndJournal` bootstraps a trie from a point-in-time snapshot and
then applies the source journal tail before replacing the live state. The
staging process is useful for a new replica or a node recovering after journal
retention has made a full replay impractical.

## Usage

The HTTP adapter composes the existing snapshot and journal pull endpoints:

```go
source := hatCache.HTTPCommandJournalJoinSource{
    Source:    "https://cache-source.example",
    AuthToken: os.Getenv("HATRIE_REPLICATION_TOKEN"),
    Pull: hatCache.CommandJournalPullOptions{
        WireFormat: hatCache.CommandJournalWireFormatBinary,
    },
}

result, err := hatCache.JoinFromSnapshotAndJournal(
    ctx,
    targetTrie,
    targetJournal,
    source,
    hatCache.SnapshotJoinOptions{
        FencingToken: 17,
        ValidateFence: func(ctx context.Context, token uint64) error {
            return lease.Check(ctx, token)
        },
    },
)
```

`CommandJournalJoinSource` can be implemented directly for object storage,
gRPC, or a local backup repository. Its snapshot method writes the downloaded
snapshot to the supplied path and returns its checkpoint. Its journal method
applies the ordered tail to the supplied staged trie and journal.

## Guarantees

- The target trie and target journal are untouched if snapshot download,
  snapshot validation, journal pull, checkpoint validation, or fence validation
  fails.
- The snapshot metadata returned by the source must match the metadata read
  from the downloaded file.
- The journal pull must start at exactly the snapshot checkpoint and cannot
  report a regressed applied checkpoint.
- The joined snapshot is written with the final applied checkpoint before
  activation. Activation uses `CommandJournal.ReplaceWithSnapshot`, which
  resets the target journal to that checkpoint.
- Non-zero fencing tokens require a validator. The validator runs immediately
  before activation; the caller should keep its lease/write exclusion valid
  through the activation call.
- Staging uses a temporary `hatrie-snapshot-join-*` directory and removes it
  on every normal return path. The test suite verifies this cleanup.

The join is an explicit bootstrap operation. It does not start a background
replication loop, and it does not silently overwrite a target journal without
the caller choosing to activate the result.

## Benchmark

The benchmark compares the new coordinator with the equivalent manual
composition of the existing public primitives: download snapshot, load a
staged trie, pull the journal, write the joined snapshot, and call
`ReplaceWithSnapshot`. It uses one small snapshot plus one journal mutation on
an AMD Ryzen 9 5950X, Linux amd64, Go 1.26.6.

Command:

```text
make benchmark-tu09-snapshot-join
```

Raw samples (`-benchtime=2s -count=3`):

| Path | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Baseline manual | 5,677,518 | 414,921 | 236 |
| Baseline manual | 7,385,130 | 399,877 | 236 |
| Baseline manual | 5,835,131 | 406,459 | 236 |
| Coordinator | 5,734,986 | 411,700 | 237 |
| Coordinator | 6,098,027 | 427,728 | 237 |
| Coordinator | 5,610,748 | 399,048 | 237 |

Median comparison is approximately **1.7% faster**, **1.3% more bytes**, and
**0.4% more allocations** for the coordinator in this run. An earlier run was
about 20% higher in bytes because the coordinator reread the final snapshot
metadata after writing it; activation already validates that metadata, so the
duplicate read was removed and the focused tests were rerun. The remaining
wall-clock difference is within ordinary temporary-disk variance for this
small workload; larger snapshots and journal tails need separate measurement.

## Verification

Focused tests cover successful activation, fence enforcement, cancellation,
metadata mismatch, journal regression, live-state preservation on pull failure,
and temporary-directory cleanup:

```text
make codex-tu09-run-stable-test
```
