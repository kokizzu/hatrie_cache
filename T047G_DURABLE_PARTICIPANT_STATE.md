# T047g Durable Participant State

`ClusterWriteCommitParticipant` already provides deterministic HCP1 snapshots
and atomic in-memory restore. T047g adds an importable local file store for
callers that need a participant's prepared, committed, or aborted decisions to
survive a process restart.

## API

```go
store, err := hatReplication.NewClusterWriteCommitParticipantFileStore(
    hatReplication.ClusterWriteCommitParticipantFileStoreOptions{
        Path: "/var/lib/hatrie/participant.state",
    },
)
if err != nil {
    return err
}

if err := store.Save(ctx, participant); err != nil {
    return err
}

found, err := store.Load(ctx, participant)
```

`Load` returns `found=false` when the file does not exist. A corrupt or
oversized file returns an error and leaves the supplied participant unchanged.
Callers decide when to save after prepare, commit, abort, or reconciliation;
the normal participant mutation path does not perform file I/O automatically.

## File guarantees

- The HCP1 participant snapshot is wrapped in an HCPF1 envelope with a bounded
  payload length and CRC32C (Castagnoli) checksum.
- Saves write a private temporary file in the target directory, `fsync` it,
  atomically rename it, and `fsync` the directory.
- New parent directories use mode `0700`; the state file uses mode `0600`.
- Loads bound allocation before decoding and validate the envelope before
  calling the participant's atomic `RestoreSnapshot`.
- The default envelope limit is the existing 64 MiB participant snapshot limit
  plus the fixed envelope header. A smaller caller limit can be configured.

The checksum detects torn or corrupted local state; it is not authentication or
encryption. The caller owns path permissions, disk placement, key management,
and any remote transport authentication.

## Measured tradeoff

Command: `make benchmark-tu47-participant-store`.

Five samples were collected on Linux/amd64 with `-benchmem`:

| Operation | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Existing in-memory HCP1 marshal | 155.8 | 184 | 3 |
| Durable atomic save | 1,438,291 | 1,607 | 20 |
| Verified file load and restore | 7,290 | 1,552 | 8 |

Durable save is intentionally much slower than memory-only snapshot creation
because it includes temporary-file creation, write, two sync boundaries, and an
atomic rename. This cost is paid only when the caller requests a durability
checkpoint; prepare, commit, abort, and ordinary replication remain unchanged.

Raw output is recorded in [BENCHMARK.md](BENCHMARK.md).
