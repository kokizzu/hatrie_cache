# MZ-003 Hydration Progress

MZ-003 is partially adopted. Snapshot hydration now exposes opt-in progress
and cancellation while retaining the existing atomic staged cutover. A
persistent resume checkpoint is still an open follow-up because resuming in
the middle of a snapshot needs a format-level record boundary and a durable
source identity.

## API

```go
metadata, err := trie.LoadSnapshotWithProgress(snapshotPath, func(progress hatCache.SnapshotRestoreProgress) error {
	log.Printf("hydrated %d entries, %d/%d bytes", progress.EntriesRead, progress.BytesRead, progress.TotalBytes)
	return nil
})
```

The callback runs once per decoded snapshot entry. `BytesRead` is the number
of bytes consumed from the snapshot file at the time of the update, and
`TotalBytes` is the file size. For compressed formats, byte progress refers to
the compressed input file, not the decompressed payload. Returning an error
stops hydration and discards the staged generation; the live trie is not
cut over.

`LoadSnapshot` and `LoadSnapshotWithMetadata` remain the default fast path. A
nil callback delegates directly to the existing implementation, so callers
that do not need progress do not pay the counting-reader or callback cost.

## Measurement

Workload: binary snapshot containing 256 string entries, five 500 ms benchmark
 samples on Linux/amd64, AMD Ryzen 9 5950X.

| Path | Median time | Median heap | Allocs | Result |
| --- | ---: | ---: | ---: | --- |
| Existing restore, before change | 515,209 ns/op | 112,501 B/op | 1,075 | Baseline |
| Existing restore, after change | 504,029 ns/op | 112,497 B/op | 1,075 | Default path unchanged within run variance |
| Opt-in progress callback | 514,944 ns/op | 112,786 B/op | 1,077 | About 2.2% slower and +295 B/+2 allocs versus post-change default |

The callback path is intentionally opt-in. The small overhead buys entry-level
progress and cancellation without changing normal restore behavior.

## Verification

- `make test-mz003-snapshot-progress`
- `make benchmark-mz003-snapshot-progress-baseline`
- `make benchmark-mz003-snapshot-progress`
- `make verify-mz003-snapshot-progress`

Broader `go test ./hat/hatCache -count=1` and `go test -race ./hat/hatCache
-count=1` runs also reached two unrelated existing SQL `EXPLAIN` assertion
failures in this worktree; the MZ-003 tests passed in both runs.
