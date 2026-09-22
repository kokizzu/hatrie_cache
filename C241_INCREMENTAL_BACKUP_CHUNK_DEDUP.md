# C241 Incremental Backup Chunk Deduplication

C241 stores large files in a content-addressed incremental backup repository
as fixed-size chunks. A later checkpoint can reuse unchanged chunks even when
the containing file changed. Small files remain whole-file objects to avoid
metadata and hashing overhead.

## Configuration

`BackupBundleOptions.RepositoryChunkSize` controls incremental repository
chunking:

```go
options := hatCache.BackupBundleOptions{
    Mode:                hatCache.BackupModePebbleIncremental,
    PersistentStore:     store,
    DirtyTracker:        tracker,
    RepositoryChunkSize: 1 << 20, // 1 MiB
}
```

- `0` uses `hatCache.DefaultBackupRepositoryChunkSize` (1 MiB).
- Values from 4 KiB through 64 MiB are accepted.
- `hatCache.DisableBackupRepositoryChunking` (`-1`) selects the legacy
  whole-file object layout.

The manifest keeps the original file size and checksum and adds ordered
`offset`, `size`, and `sha256` chunk declarations. Restore verifies every
chunk and then verifies the reconstructed whole-file checksum. Existing
whole-file manifests continue to restore unchanged. Retention counts each
unique reachable chunk object, so byte budgets remain physical-object budgets.

## Tradeoff

Fixed boundaries are deliberately conservative. They add one hash pass and
more object metadata, and an insertion near the beginning can still change all
following chunks. Content-defined chunking could improve that case but would
add CPU, implementation complexity, and more attack surface, so it is not part
of C241.

Measured with `make benchmark-c241` on Linux/amd64, AMD Ryzen 9 5950X, five
samples per case, 10,000 keys of 256 bytes, and 1% changed per incremental
backup:

| Path | Median time | Median new object bytes | Median written bytes | Median memory | Median allocs | Relative result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Whole-file control | 23.438 ms | 1,101,804 | 1,105,467 | 1,491,274 B/op | 3,477 | baseline |
| 1 MiB chunked default | 25.091 ms | 200,328 | 204,322 | 1,521,308 B/op | 3,356 | 1.07x time; 5.50x fewer new bytes; 5.41x fewer written bytes; 1.02x memory |

The chunked default spends about 7% more backup CPU on this workload to cut
the transferable payload by about 81.9%. Use the legacy switch when CPU is
more important than repository transfer/storage reduction or when compatibility
with a whole-file object layout is required.

### Raw output

```text
Chunked1MiB:       37.673229, 25.091461, 26.378954, 25.082767, 24.838594 ms/op
Chunked1MiB:       200328, 100164, 467432, 267104, 100164 new_object_B/op
WholeFileControl:  24.051669, 23.078073, 24.093990, 23.411704, 23.438107 ms/op
WholeFileControl:  1402296, 1101804, 534208, 1135192, 734536 new_object_B/op
```

The complete benchmark output is produced by the Make target and is not
checked in as a generated artifact.
