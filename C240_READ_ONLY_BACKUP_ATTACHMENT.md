# C240 Read-Only Backup Attachment

C240 adds two read-only ways to inspect an immutable backup without restoring
it into a writable data directory.

## Object-store backup parts

`ObjectStoreTarget.AttachReadOnly` downloads and validates `manifest.json` only.
It does not create a staging directory and it does not call `Put`.

```go
target, err := hatBackup.NewObjectStoreTarget(store, "backups/orders")
if err != nil {
    return err
}
attachment, err := target.AttachReadOnly(ctx)
if err != nil {
    return err
}

for _, file := range attachment.Files() {
    reader, err := attachment.Open(ctx, file.Path)
    if err != nil {
        return err
    }
    if _, err := io.Copy(destination, reader); err != nil {
        _ = reader.Close()
        return err
    }
    if err := reader.Close(); err != nil {
        return err
    }
}
```

`Open` accepts only an exact relative path listed by the manifest. It rejects
absolute paths, traversal, backslashes, unknown files, and reserved manifest
paths. The returned stream checks the declared size and SHA-256 when it reaches
EOF. A caller that reads only a prefix gets streaming behavior but must call
`Verify` when complete integrity validation is required.

Use `ReadFile` for small parts when buffering is acceptable:

```go
rows, err := attachment.ReadFile(ctx, "part/rows.bin")
```

Use `Verify` to checksum every manifest-listed object without producing a
restore directory:

```go
if err := attachment.Verify(ctx); err != nil {
    return err
}
```

The attachment API has no mutation or restore method. The underlying object
store may still be writable through a separate handle, so this is an API-level
read-only view, not an access-control boundary for the store itself.

## Local Pebble checkpoint

For a local, self-contained Pebble checkpoint, use the public read-only opener:

```go
store, err := hatCache.OpenPebbleStoreReadOnlyWithFormat(
    checkpointPath,
    hatCache.StorageFormatBinary,
)
if err != nil {
    return err
}
defer store.Close()

trie := hatCache.CreateHatTrie()
if _, err := store.Load(trie); err != nil {
    return err
}
```

The cipher-aware constructor is available as
`OpenPebbleStoreReadOnlyWithFormatAndCipher`. Read-only opening does not create
missing directories, recover checkpoint-adoption markers, delete old
generations, or enable `Save`, `SaveKeys`, or checkpoint mutation. Pebble
returns an error if a write is attempted.

## Benchmark

Measured on Linux amd64 with an AMD Ryzen 9 5950X, a 64 KiB payload, and an
in-memory object store. Restore writes to the local filesystem; attachment
reads the same object and verifies its checksum. Five benchmark samples were
run and the median is reported.

| Operation | Median ns/op | B/op | allocs/op | Compared with full restore |
| --- | ---: | ---: | ---: | ---: |
| Full restore to disk | 1,937,206 | 42,072 | 112 | 1.00x |
| Attach + `ReadFile` | 70,532 | 141,168 | 49 | 27.47x faster |
| Reused attach + `ReadFile` | 66,427 | 138,640 | 26 | 29.16x faster |
| Attach + streaming `Open` | 39,559 | 3,064 | 33 | 48.97x faster |
| Reused attach + streaming `Open` | 33,474 | 529 | 10 | 57.87x faster |

For the streaming path, transient benchmark allocation is 13.73x lower for a
fresh attachment and 79.53x lower when the attachment is reused. `ReadFile`
uses 3.36x more bytes than full restore because it deliberately buffers the
64 KiB result; use `Open` for large parts. These numbers exclude network
latency and object-store transfer costs, so production measurements should be
repeated against the real store.

Raw `ns/op` samples from `make benchmark-c240`:

| Operation | Five samples |
| --- | --- |
| Full restore to disk | 1,895,121; 1,998,553; 1,937,206; 1,876,915; 2,003,355 |
| Attach + `ReadFile` | 70,924; 68,421; 71,407; 70,532; 69,296 |
| Reused attach + `ReadFile` | 64,666; 63,056; 68,458; 66,427; 67,575 |
| Attach + streaming `Open` | 38,148; 39,854; 39,559; 41,517; 39,116 |
| Reused attach + streaming `Open` | 33,474; 32,761; 35,909; 32,908; 33,610 |
