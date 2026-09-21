# C241 Incremental Backup Chunk Deduplication

Incremental Pebble backup repositories now deduplicate large payloads at the
chunk level. A changed middle range no longer forces the complete checkpoint
file to become a new content-addressed object.

## Configuration

`BackupBundleOptions.RepositoryChunkSize` applies to
`BackupModePebbleIncremental` repositories:

| Value | Behavior |
| --- | --- |
| `0` | Default: 1 MiB chunks |
| Positive value `>= 4096` | Use that fixed chunk size in bytes |
| `BackupRepositoryChunkingDisabled` (`-1`) | Keep the legacy whole-file object layout |

Only files larger than the selected chunk size are chunked. Small files keep
the existing one-object representation. Example:

```go
manifest, err := CreateBackupBundle(repository, trie, nil, BackupBundleOptions{
    Mode:                 BackupModePebbleIncremental,
    PersistentStore:      store,
    DirtyTracker:         tracker,
    RepositoryChunkSize:  2 << 20,
})
```

Use `RepositoryChunkSize: BackupRepositoryChunkingDisabled` when an existing
whole-file repository layout is required for compatibility or operational
simplicity.

## Manifest and Restore

`BundleFile.Chunks` is optional. When present, ordered chunks contain
contiguous `offset`, `size`, and SHA-256 fields covering the complete file;
the file's original full SHA-256 remains the final integrity check. Manifests
without `chunks` remain readable and restore through the legacy object path.

Restore verifies every chunk object, reconstructs the file, and verifies the
full-file checksum. Resume restore uses the same verification path. Retention
and backup-chain planning account for chunk hashes and physical chunk sizes,
so pruning cannot remove a chunk referenced by a retained manifest.

Chunk metadata is validated before restore or chain planning. Unsafe offsets,
gaps, overlaps, invalid hashes, and incomplete coverage are rejected.

## Tradeoffs

Chunking reads and hashes the complete source file on each snapshot, even when
most chunks are reused. It creates more object references and can increase
metadata and allocation count. The default buffer is bounded to 1 MiB and is
reused through a pool; chunking is not enabled for small files. The `-1`
fallback remains available when CPU or metadata overhead is more important
than incremental transfer/storage volume.

## Benchmark

Run with:

```text
make benchmark-c241-backup-chunk-dedup
```

Five `-benchmem` samples ran on Linux/amd64 with an AMD Ryzen 9 5950X. Each
sample performs one changed-snapshot write after an 8 MiB base payload. The
changed 64-byte range stays inside one 1 MiB chunk.

| Path | Median ns/op | Median B/op | Median allocs/op | New object bytes | Relative |
| --- | ---: | ---: | ---: | ---: | ---: |
| Legacy whole-file objects | 21,855,699 | 40,456 | 45 | 8,388,608 | 1.00x |
| Default 1 MiB chunks | 17,694,767 | 12,640 | 94 | 1,048,576 | 1.24x faster, 8.00x lower changed bytes |

Raw output from the measured run:

```text
BenchmarkC241BackupRepositoryWholeFileBaseline-32  1  27083660 ns/op  8388608 new-object-bytes  40480 B/op 46 allocs/op
BenchmarkC241BackupRepositoryWholeFileBaseline-32  1  23839212 ns/op  8388608 new-object-bytes  40480 B/op 46 allocs/op
BenchmarkC241BackupRepositoryWholeFileBaseline-32  1  21855699 ns/op  8388608 new-object-bytes  40456 B/op 45 allocs/op
BenchmarkC241BackupRepositoryWholeFileBaseline-32  1  18433946 ns/op  8388608 new-object-bytes  40456 B/op 45 allocs/op
BenchmarkC241BackupRepositoryWholeFileBaseline-32  1  18586231 ns/op  8388608 new-object-bytes  40432 B/op 44 allocs/op
BenchmarkC241BackupRepositoryChunked-32            1  15386675 ns/op  1048576 new-object-bytes  12640 B/op 93 allocs/op
BenchmarkC241BackupRepositoryChunked-32            1  17573234 ns/op  1048576 new-object-bytes  12640 B/op 93 allocs/op
BenchmarkC241BackupRepositoryChunked-32            1  17713678 ns/op  1048576 new-object-bytes  12680 B/op 94 allocs/op
BenchmarkC241BackupRepositoryChunked-32            1  18943020 ns/op  1048576 new-object-bytes  12664 B/op 94 allocs/op
BenchmarkC241BackupRepositoryChunked-32            1  17694767 ns/op  1048576 new-object-bytes  12520 B/op 94 allocs/op
```

The measured path uses one new chunk and seven reused chunks. Allocation count
is about 2x higher because each content-addressed chunk is checked separately,
but total allocated bytes are lower after pooling the bounded buffer. The
feature is therefore enabled by default for large files, with the legacy mode
available as an explicit fallback.
