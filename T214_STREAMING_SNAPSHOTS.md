# T214 Streaming Snapshots

T214 adds a transport-neutral snapshot pull for replicas that cannot access the
source node's filesystem. `StreamCommandJournalSnapshot` authenticates the
existing `/api/journal/snapshot` endpoint and forwards the response directly to
an `io.Writer`; it does not create a local temporary snapshot.

## API

```go
var destination io.Writer = remoteStagingWriter
manifest, err := hatCache.StreamCommandJournalSnapshot(
    ctx,
    "https://source.example",
    replicationToken,
    httpClient,
    destination,
    requiredJournalSequence,
)
if err != nil {
    return err
}
// Commit the remote staged object only after the writer and manifest checks
// have succeeded.
```

The request carries the same replication authentication headers as the
existing pull path. The source response now includes:

- `X-Hatrie-Journal-Sequence`;
- `X-Hatrie-Snapshot-Format`;
- `X-Hatrie-Snapshot-SHA256`;
- the normal `Content-Length` and snapshot content type.

The client validates the status, sequence floor, format, content length, and
SHA-256 while forwarding bytes. A sequence that is too old is rejected before
the body is copied. A writer error or final digest mismatch can leave a
partially written destination, so the caller must stage to a remote object and
discard it on error before publishing it as a replica snapshot.

`PullCommandJournalSnapshot` remains the local atomic-file API. It is unchanged
for callers that want a filesystem snapshot and local rename semantics.

## Measurement

Three `-benchmem` samples were collected on Linux/amd64 with an AMD Ryzen 9
5950X over the same 128-key gzip-binary snapshot payload and local HTTP
transport. The baseline materializes to a local file, including the existing
atomic temporary-file, fsync, reopen, and metadata scan. The stream path writes
to `io.Discard` to isolate transfer and validation work; network-writer cost is
owned by the supplied writer.

| Path | Raw ns/op samples | Median ns/op | Memory/op | Allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Existing materializing pull | `17342788 54428812 34937345` | `34937345` | `153677 B` | `655` | `1.00x` |
| Direct stream + digest validation | `110261 112500 110856` | `110856` | `41158 B` | `97` | `315.3x faster` |

The stream path retains identical snapshot wire bytes and adds digest/header
validation, while avoiding local disk I/O. It reduces this workload by about
`99.7%` CPU time, `73.2%` memory/op, and `85.2%` allocations. The tradeoff is
that streaming is not itself an atomic filesystem publication; the receiving
replica must provide a transactional remote writer or staging/commit protocol.

Commands:

```text
make test-t214
make benchmark-t214-baseline
make benchmark-t214
make race-t214
make vet-t214
```
