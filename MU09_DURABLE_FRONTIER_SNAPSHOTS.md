# M-U09 Durable Frontier Snapshots

`hatPipeline.FrontierRegistry` already has a deterministic, versioned binary
snapshot format. M-U09 adds a durable file store and registry helpers around
that format without changing the existing in-memory or caller-owned store
interfaces.

## API

```go
store, err := hatPipeline.NewFrontierSnapshotFileStore(
    hatPipeline.FrontierSnapshotFileStoreOptions{
        Path: "/var/lib/hatrie/frontiers.bin",
    },
)
if err != nil {
    return err
}

if err := registry.SaveDurableSnapshot(ctx, store); err != nil {
    return err
}

restored, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
if err != nil {
    return err
}
found, err := restored.RestoreDurableSnapshot(ctx, store)
```

`RestoreDurableSnapshot` returns `found=false, err=nil` when the file does not
exist. A present but malformed file returns `ErrFrontierSnapshotInvalid`; a
registry that already contains frontiers returns
`ErrFrontierSnapshotNotEmpty`. The existing restore decoder validates the
complete payload before mutating the registry.

Applications may implement `FrontierSnapshotStore` for a WAL, object store, or
database. Its `Load` method uses a nil payload to mean missing, and its `Save`
method must return only after its own durability boundary. The file store is a
reference local implementation; it does not create the parent directory.

## Durability And Security

The file store writes a same-directory temporary file with mode `0600`, flushes
the file, atomically renames it over the destination, and flushes the parent
directory. This prevents readers from observing a partial payload and makes a
successful save a completed rename plus directory flush. A context canceled
before completion returns its cancellation error; cancellation racing with the
final flush can still leave the newly renamed snapshot on disk, so callers
must treat a returned error as requiring their normal retry/recovery policy.

`MaxBytes` defaults to the existing 64 MiB binary snapshot bound and can only
be lowered. Reads validate the file size before allocating. The store does not
follow a destination symlink during replacement: rename replaces the directory
entry, which avoids writing through an attacker-controlled symlink. Operators
must still protect the parent directory and use a trusted path.

For a crash-safe source checkpoint, persist the source/WAL position and the
frontier snapshot according to the application's commit protocol. This API
does not claim a source offset is durable merely because the frontier file was
written.

## Measurement

Commands:

```sh
make test-mu09
make verify-mu09
make benchmark-mu09-baseline
make benchmark-mu09
```

Five samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture contains 128
named frontiers. Durable save includes file write, file sync, rename, and
parent-directory sync. Durable restore includes file read, binary decoding, and
creation of a fresh registry.

| Workload | ns/op samples | Median ns/op | B/op | allocs/op | Interpretation |
| --- | --- | ---: | ---: | ---: | --- |
| Existing binary marshal control | 21,704; 23,774; 23,714; 22,562; 22,818 | 22,818 | 17,208 | 5 | In-memory encoding only |
| Durable file save | 1,895,569; 1,712,404; 1,695,605; 5,419,584; 1,609,443 | 1,712,404 | 18,512 | 22 | Durability boundary included |
| Existing binary decode control | 13,301; 13,048; 13,319; 12,930; 13,755 | 13,301 | 18,088 | 132 | In-memory decoding only |
| Durable file restore | 39,501; 40,028; 40,181; 39,104; 39,174 | 39,501 | 45,752 | 279 | Fresh-registry recovery |

Durable save is about 75x slower than marshal-only in this local benchmark and
uses about 8% more measured bytes and 17 more allocations. Durable restore is
about 3x slower than decode-only and uses about 2.5x the measured bytes. Those
are intentional control-plane costs for crash-safe persistence, not a claim of
hot-path improvement. Keep snapshots at checkpoint boundaries rather than on
every row update, and use `MarshalSnapshot` directly when the caller already
has its own durable transaction.

