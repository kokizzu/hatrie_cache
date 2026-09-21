# MZ-049 Exactly-Once Snapshot Export

`CommandJournal.WriteSnapshotWithResumableExport` is an opt-in local-filesystem
export path for snapshots that may be interrupted during publication.

## Usage

```go
report, err := journal.WriteSnapshotWithResumableExport(
    trie,
    "/var/backups/cache/snapshot.hc",
    hatCache.SnapshotExportOptions{
        Format: hatCache.SnapshotFormatGzipBinary,
    },
)
if err != nil {
    return err
}
fmt.Println(report.Manifest.SHA256, report.BytesWritten)
```

The normal `SaveSnapshot` and `WriteSnapshotWithManifest` APIs are unchanged.
When a checkpoint already exists, a subsequent call resumes automatically.
Pass `CheckpointPath` when the sidecar must be stored outside the default
`<target>.resume.json` path.

## Recovery Semantics

The exporter first creates an immutable source snapshot and its manifest. It
then copies the source to a partial target in 1 MiB chunks. Each chunk is
synced before the checkpoint advances, and the checkpoint stores the committed
byte count plus the SHA-256 digest of the committed prefix.

The sidecars are:

- `<target>.resume-source`: immutable staged snapshot
- `<target>.resume-partial`: committed output prefix plus any uncommitted tail
- `<target>.resume.json`: target, format, manifest, prefix digest, and progress

On retry, a tail beyond the committed checkpoint is truncated, the committed
prefix is verified, and copying resumes at that exact byte. The completed
output is manifest-verified and atomically renamed into place. Source,
partial, and checkpoint files are removed only after publication.

The implementation rejects malformed checkpoints and symlinked or non-regular
source/partial paths. Files and generated directories use the existing
0600/0700 local-storage permissions.

This is a local export primitive, not a network protocol. A destination
filesystem can still be a mounted or externally managed filesystem, but its
rename and sync guarantees determine the final durability boundary.

## Tradeoff

The resumable path is deliberately opt-in because it stages and verifies the
payload and fsyncs progress. The direct path remains the default.

On an AMD Ryzen 9 5950X, 256 string keys, binary snapshots, three runs with
`-benchtime=500ms`:

| Path | Median time | Allocated bytes | Allocs | Payload I/O |
| --- | ---: | ---: | ---: | ---: |
| Direct atomic export | 1.75 ms | 141,328 B/op | 795 | 10,504 B |
| Resumable export | 7.07 ms | 324,472 B/op | 1,957 | 21,008 B |

The recovery guarantee costs about 4.04x CPU time, 2.30x allocation bytes, and
2x payload I/O in this small local benchmark. It should therefore be used for
backup/export workflows where retryability is more important than the fastest
single local write.

Focused verification is available with:

`make test-mz049-snapshot-export`

`make race-mz049-snapshot-export`

`make benchmark-mz049-snapshot-export`
