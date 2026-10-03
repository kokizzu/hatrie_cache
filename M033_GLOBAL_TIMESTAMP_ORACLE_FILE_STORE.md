# M033c Global Timestamp Oracle File Store

`hatReplication.GlobalTimestampOracle` already provides globally ordered
timestamp ranges and a deterministic in-memory snapshot. This addition gives a
caller-controlled crash-safe checkpoint store for that snapshot.

## Usage

```go
store, err := hatReplication.NewGlobalTimestampOracleFileStore(
	 hatReplication.GlobalTimestampOracleFileStoreOptions{
		Path: "/var/lib/hatrie-cache/timestamp-oracle/state.bin",
	},
)
if err != nil {
	return err
}

if err := store.SaveOracle(oracle); err != nil {
	return err
}

restored, err := store.LoadOracle()
if err != nil {
	return err
}
```

For a consensus log or caller-owned state machine, use `SaveSnapshot` and
`LoadSnapshot` to bind the checkpoint to the surrounding commit boundary.
`SaveOracle` and `LoadOracle` are convenience wrappers around the existing
`Snapshot` and `NewGlobalTimestampOracleFromSnapshot` APIs.

## Format And Safety

- `MarshalGlobalTimestampOracleSnapshot` uses deterministic `GTO1` binary
  framing, canonical unsigned varints, sorted node records, and CRC32C.
- `UnmarshalGlobalTimestampOracleSnapshot` rejects unknown magic, truncated or
  noncanonical input, checksum failures, duplicate or unsorted nodes, range
  overlap, invalid epochs, and timestamp overflow.
- The default file limit is 8 MiB; the hard limit is 64 MiB. `MaxBytes` can be
  lowered per store.
- Saves use a private temporary file, `0600` permissions, file `fsync`, atomic
  same-directory rename, and directory `fsync`.
- A newly created parent directory uses `0700` permissions. Existing regular
  files are accepted; symlink state paths and path swaps are rejected.
- Missing files retain `os.IsNotExist` behavior so bootstrap policy remains
  caller-owned.

## Scope And Tradeoff

The store is explicit and opt-in. `Reserve`, `GlobalTimestampLease.Next`, and
normal snapshot reads perform no file I/O. A save is intentionally much slower
than serialization because it synchronizes the file and containing directory;
callers should save at a consensus/checkpoint boundary, not for every reserved
timestamp. JSON remains available as an explicit human-readable fallback.

See [BENCHMARK.md](BENCHMARK.md#m033c-global-timestamp-oracle-binary-codec-and-durable-checkpoints)
for raw measurements.
