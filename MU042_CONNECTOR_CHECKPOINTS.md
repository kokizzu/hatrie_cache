# Connector Pause/Resume Checkpoints

M-U42 adds an opt-in checkpoint boundary to `hatPipeline.ConnectorRegistry`.
It pauses a running connector, records the lifecycle generation, and saves an
opaque source offset/frontier payload before returning success. A restore
loads and validates that payload, applies it while the connector is paused,
then resumes the connector.

## API

```go
store, err := hatPipeline.NewFrontierSnapshotFileStore(
    hatPipeline.FrontierSnapshotFileStoreOptions{
        Path: "/var/lib/my-service/orders.checkpoint",
    },
)
if err != nil {
    return err
}

checkpoint, err := registry.PauseWithCheckpoint(ctx, "orders", hatPipeline.ConnectorCheckpoint{
    Sequence:  42,
    Offset:    encodeSourceOffset(offset),
    Frontier:  encodeSourceFrontier(frontier),
}, store)
if err != nil {
    return err
}

_, err = registry.ResumeFromCheckpoint(ctx, "orders", store,
    func(ctx context.Context, checkpoint hatPipeline.ConnectorCheckpoint) error {
        offset, frontier, err := decodeSourcePosition(checkpoint.Offset, checkpoint.Frontier)
        if err != nil {
            return err
        }
        return source.RestorePosition(ctx, offset, frontier)
    },
)
```

`Offset` and `Frontier` are source-specific bytes. `hatPipeline` does not
depend on `hatSql`, Kafka, CDC, or a particular offset representation. The
restore callback runs before `Connector.Resume`; it must not call lifecycle
methods on the same registry.

An empty `ConnectorID` is filled from the method's `id`. An explicit different
ID is rejected. `Sequence` must be non-zero. The registry supplies
`Generation` after a successful pause, so callers must leave it zero when
creating a new checkpoint.

## Recovery Contract

- A successful pause is not reported until `store.Save` returns nil.
- A save failure leaves the connector paused. This prevents a process from
  resuming without a durable position; an old checkpoint is not accepted
  because its generation is stale.
- Restore requires the connector to still be paused and the stored generation
  to equal the current lifecycle generation.
- Any start, pause, resume, stop, or failed transition advances the generation.
  This fences stale operator commands after a restart or manual intervention.
- A missing, corrupt, oversized, or foreign checkpoint is rejected before the
  source restore callback runs.
- Existing bounded lifecycle history records the pause and resume transitions;
  checkpoint `Sequence` provides the caller's source/audit sequence.

The supplied `FrontierSnapshotFileStore` uses a bounded payload, a private
`0600` file, same-directory temporary output, fsync, atomic rename, and parent
directory sync. Its parent directory must already exist and should be owned by
the service account.

## HCP1 Format

`EncodeConnectorCheckpoint` and `DecodeConnectorCheckpoint` use a deterministic
HCP1 binary payload with big-endian fixed-width lengths, opaque offset/frontier
fields, and CRC32C. Connector IDs are bounded to 256 bytes, each opaque field
to 512 KiB, and the complete payload to 1 MiB. Decode checks all bounds and the
checksum before allocating field buffers.

The binary format is the only format of this API. JSON is used below only as a
benchmark baseline; it is not accepted as a checkpoint payload.

## Verification And Benchmark

```sh
make test-mu42
make verify-mu42
make benchmark-mu42-baseline
make benchmark-mu42
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. HCP1 and JSON used the same
connector ID, sequence, generation, offset, and frontier values.

| Workload | JSON median | HCP1 median | CPU improvement | JSON B/op | HCP1 B/op | JSON allocs/op | HCP1 allocs/op | Wire JSON/HCP1 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Encode | 309.8 ns | 57.73 ns | 5.36x faster | 240 | 112 | 2 | 1 | 160 B / 98 B, 1.63x smaller |
| Decode | 1,677 ns | 87.75 ns | 19.1x faster | 376 | 80 | 8 | 3 | 160 B / 98 B, 1.63x smaller |

The complete in-memory lifecycle benchmark is intentionally different from a
plain pause/resume control: the baseline median was 143.1 ns/op, 0 B/op, and
0 allocs/op; pause/resume with checkpoint persistence and restore measured
592.2 ns/op, 424 B/op, and 12 allocs/op (4.14x slower). That cost is the
optional durability and source-restore work, not overhead added to the normal
`Pause` or `Resume` methods. Filesystem durability adds the configured store's
write and fsync cost and was not folded into the in-memory comparison.

Focused tests cover round trips, callback ordering, stale generation fencing,
save failure, missing/corrupt/foreign payloads, bounds, CRC32C, the existing
atomic file store, race detection, and vet.
