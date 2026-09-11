# Persistent Storage Disk Reserve

Hatriecache can optionally keep a physical free-space reserve on the filesystem
that contains a LevelDB or Pebble persistent store. Before a persistent write,
the store checks the filesystem's available bytes and rejects the write when
the configured reserve would be crossed.

## Configuration

The command-line server keeps this protection disabled by default:

```text
-db-storage-disk-reserve-bytes 10737418240
```

The value is a byte count. `0` disables the check. A negative value is invalid,
and a positive value requires `-db-path`.

The library API is available from the root package:

```go
if err := hatriecache.ConfigurePersistentStoreDiskReserveBytes(store, 10<<30); err != nil {
    return err
}
reserve := hatriecache.PersistentStoreDiskReserveBytes(store)
```

The setting is process-local and is not persisted in the store. It must be
configured again after reopening a store.

## Scope And Errors

The guard applies to LevelDB and Pebble full saves, key saves, dirty saves, and
the Pebble generation-save path used by full checkpoints. It is separate from
the logical `-db-storage-max-bytes` limit: the logical limit measures store
contents, while this setting protects physical filesystem headroom.

When the reserve is crossed, the operation returns
`ErrPersistentStorageDiskReserveExceeded`. When free-space inspection fails,
the operation returns `ErrPersistentStorageDiskReserveUnavailable`; enabled
checks fail closed. The filesystem probe uses the available-block count for the
store path. Unsupported platforms also fail closed when a positive reserve is
enabled.

This is an admission guard, not a garbage collector. It does not create space,
compact old data, or reserve bytes for another process. Leave it at `0` when
the deployment has another disk-capacity controller or intentionally accepts
the default behavior.

## Measured Cost

The paired benchmark repeatedly saved a small Pebble store. The enabled case
used a one-byte reserve so every save passed the check.

| Mode | Median time | Bytes/op | Allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Disabled (`reserve=0`) | 6,787,756 ns | 373,085 | 1,405 | baseline |
| Enabled (`reserve=1`) | 7,011,204 ns | 371,397 | 1,403 | 1.03x slower |

The measured difference is about 3.3% slower in this noisy write benchmark,
with no meaningful increase in allocation count or bytes/op. With the default
of `0`, the filesystem probe is skipped. See the raw five-run output in
[BENCHMARK.md](BENCHMARK.md).
