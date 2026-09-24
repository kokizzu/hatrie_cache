# C241: Incremental Backup Chunk Deduplication

C241 adds optional fixed-size content-addressed chunks to incremental Pebble
object-store backups. It is useful when successive snapshots change only small
regions of large files: unchanged chunks are reused and only new chunk payloads
cross the object-store write boundary.

## Configuration

Chunking is disabled by default. The existing whole-file object layout remains
the compatibility path:

```go
target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backups/app", hatBackup.ObjectStoreTargetOptions{
	Layout: hatBackup.ObjectStoreLayoutContentAddressed,
})
```

Enable it explicitly with a fixed chunk size:

```go
target, err := hatBackup.NewObjectStoreTargetWithOptions(store, "backups/app", hatBackup.ObjectStoreTargetOptions{
	Layout:    hatBackup.ObjectStoreLayoutContentAddressed,
	ChunkSize: 8 << 20, // 8 MiB
})
```

`ChunkSize` must be positive and no larger than `hatBackup.MaxObjectStoreChunkSize`
when enabled. A value of zero preserves whole-file objects. Chunking applies
only to content-addressed incremental Pebble backups; path-addressed snapshots
and non-incremental modes retain their existing behavior. Files no larger than
the configured chunk size also retain a single object.

Each chunk is identified by its SHA-256 digest. The manifest keeps the logical
file size and digest plus an ordered list of chunk sizes and digests. Restore,
`Verify`, read-only attachment, retention planning, and garbage-collection
planning all validate and account for those physical chunk objects.

## Choosing A Chunk Size

Smaller chunks reduce the payload for localized changes but increase manifest
metadata, existence checks, object-store requests, and per-chunk hashing.
Larger chunks reduce that overhead but cause a larger rewrite when a chunk
changes. Fixed boundaries are intentional and predictable; this is not a
content-defined chunker, so inserting bytes near the start of a file can shift
and rewrite later chunks.

The whole file is still read once to calculate its logical SHA-256 and its
chunk digests. Chunk reuse does not mean that backup can skip scanning the
source file. Use a chunk size aligned with the application's typical update
region and measure against the actual object-store latency and request pricing.

## Encryption And Integrity

Chunk objects use the existing object-store encryption configuration when it is
enabled. The encryption key identity is part of the physical content address,
so key rotation does not incorrectly reuse ciphertext under a different key.
Restore and attachment readers verify each chunk size and SHA-256, then verify
the reconstructed logical file size and SHA-256. A corrupted chunk is rejected
before a restore is published. SHA-256 provides accidental-corruption
detection; use the existing authenticated encryption and protected object-store
access controls when adversarial tampering is in scope.

## Measured Tradeoff

The focused benchmark uses a 1 MiB file with 16 distinct 64 KiB regions and a
one-byte change in the middle region. It uses an in-memory object store and
measures only the incremental `Backup` call; setup and base-backup creation are
outside the timed region. Five samples were run with:

```text
make benchmark-c241
```

| Mode | Median ns/op | Median B/op | Median allocs/op | New objects/op | Reused objects/op | Payload bytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Whole-file, `ChunkSize: 0` | 2,305,938 | 3,347,984 | 125 | 1 | 0 | 1,048,576 |
| 64 KiB chunks | 1,422,393 | 335,404 | 230 | 1 | 15 | 65,536 |

Relative to the whole-file path, this fixture measured 1.62x lower CPU time,
9.98x lower allocation bytes, and 16x lower new-object payload. It used 1.84x
more allocations per operation. The benchmark is not a universal network
throughput claim: object-store round trips, object metadata pricing, source
change patterns, compression, and encryption can change the result.

The default remains `ChunkSize: 0` because it avoids those additional metadata
and request costs for applications that do not have sparse changes, while
preserving the established manifest and object format. Enable chunking only
after measuring the intended workload.

## Verification

```text
make test-c241
make test-c241-package
make race-c241
make vet-c241
make benchmark-c241
```

The focused tests cover base and incremental deduplication, restore, verify,
read-only attachment, retention and garbage-collection accounting, metadata
copying, and rejection of corrupted chunk payloads.
