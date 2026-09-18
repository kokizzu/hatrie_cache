# Snapshot Manifest

`hatCache.CommandJournal.WriteSnapshotWithManifest` provides an integrity
manifest for an online snapshot stream. It keeps the exact journal sequence
captured by the existing snapshot barrier and adds the canonical format, bytes
written, and SHA-256 of the complete emitted stream.

## Write and verify

```go
var snapshot bytes.Buffer
manifest, err := journal.WriteSnapshotWithManifest(
    trie,
    &snapshot,
    hatCache.SnapshotFormatGzipBestBinary,
)
if err != nil {
    return err
}

if err := os.WriteFile(path, snapshot.Bytes(), 0o600); err != nil {
    return err
}
if err := hatCache.VerifySnapshotManifest(path, manifest); err != nil {
    return err
}
```

The manifest fields are:

| Field | Meaning |
| --- | --- |
| `JournalSequence` | The source sequence represented by the snapshot. Replay can continue after this value. |
| `Format` | The canonical snapshot encoding selected by the writer. |
| `SizeBytes` | Number of compressed or uncompressed bytes emitted to the writer. |
| `SHA256` | Lowercase hexadecimal digest of exactly those emitted bytes. |

`VerifySnapshotManifest` checks the file size, digest, and embedded journal
sequence before a restore workflow uses the file. A corrupted or truncated
file returns an error wrapping `ErrSnapshotManifestMismatch` when the byte
identity differs. Snapshot parsing still performs its normal structural and
semantic validation during the subsequent load.

## Atomic restore and recovery coverage

Snapshot manifests are usable with the existing staged restore publication
and resume flow. The restore tests cover semantic verification, atomic
destination replacement, preservation of the previous destination on failure,
symlink rejection, stale-file repair, and safe checkpoint retention. The
focused manifest test also passes under the race detector.

The 10k-key atomic-restore benchmark measures integrity and pass-count
behavior, not a raw latency win. On an AMD Ryzen 9 5950X, the snapshot path
used one payload pass but measured `69.0 ms` versus `58.8 ms` for the legacy
double-pass path; the checkpoint path measured `213.6 ms` versus `78.1 ms`.
The single-pass paths used fewer allocations, but their current checkpoint
implementation is slower, so atomic publication should be selected for
correctness, resumability, and failure isolation rather than throughput.

## Compatibility and cost

The existing `WriteSnapshotWithFormat` and `SaveSnapshotWithFormat` APIs are
unchanged and do not hash output. Callers opt in only when transfer or backup
integrity metadata is needed. Five local runs over a 64-entry binary snapshot:

| Path | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing writer | 47,180 | 119,072 | 202 |
| With manifest | 50,254 | 119,408 | 207 |

The measured opt-in cost was about 6.5% latency, 336 bytes, and five
allocations per snapshot in this workload. It buys a receiver-verifiable
identity and does not change the default path or snapshot bytes.
