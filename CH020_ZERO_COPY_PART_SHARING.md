# CH-020: Zero-Copy Remote Part Sharing

Status: partial adoption.

The storage package now has an opt-in, bounded registry that shares immutable
remote-part metadata by reference. It does not copy part bytes, perform network
I/O, open files, or delete objects. Actual replica transport and part lifetime
remain owned by the caller.

## API

```go
registry, err := hatStorage.NewRemotePartRegistry(hatStorage.RemotePartRegistryOptions{
    MaxEntries: 4096,
})

reference, err := hatStorage.NewRemotePartReference(
    "s3://bucket/parts/part-001.bin",
    "parts/part-001.bin",
    "sha256:...",
    4096,
)

_, err = registry.Register(hatStorage.RemotePartRegistration{
    Key:        "part-001",
    Reference:  reference,
    Generation: 1,
})
part, ok := registry.Lookup("part-001")
```

`MaxEntries == 0` uses the default of 4096. The hard upper bound is
`MaxRemotePartRegistryEntries` (1,048,576). The registry is not connected to
the normal storage path, so existing users do not incur this overhead.

## Correctness Guarantees

- A registration is metadata-only and stores the already-validated immutable
  `RemotePartReference` value.
- Repeating the same key, reference, and generation is idempotent.
- A lower generation is rejected as stale.
- A different reference at the same generation is rejected as a conflict.
- A higher generation replaces the old entry.
- Removal is generation-fenced, and missing removal is idempotent.
- Snapshots are deterministic and sorted by key.
- The registry is safe for concurrent lookup, registration, removal, length,
  and snapshot operations.

## Benchmark

Command: `make benchmark-ch020-registry`

Machine: AMD Ryzen 9 5950X, linux/amd64. Five benchmark samples were run for
each path with 1024 registered entries. The raw samples below are the observed
`ns/op` values from the final run.

| Operation | Raw map samples (ns/op) | Registry samples (ns/op) | Registry / map | Registry allocs |
| --- | ---: | ---: | ---: | ---: |
| Lookup | 15.94, 16.16, 14.54, 14.97, 14.84 | 36.55, 38.70, 37.12, 39.08, 40.57 | about 2.6x slower | 0 B, 0 allocs |
| Replace | 19.67, 19.42, 18.23, 17.96, 19.53 | 66.89, 71.57, 76.37, 73.46, 72.49 | about 3.7x slower | 0 B, 0 allocs |

The registry intentionally costs more than an unprotected map because it adds
locking, bounds, validation, generation fencing, and conflict handling. The
important result for this feature is that the valid write path does not parse
URLs or normalize paths again and does not allocate. Registration is expected
to be control-plane work; the data-plane part bytes are not copied by this
registry.

### Rejected experiment

An immutable copy-on-write map was measured and reverted. At 1024 entries,
replacing one entry cost about 103 microseconds, 213 KB, and 9 allocations,
versus about 20 ns, 0 B, and 0 allocations for the raw map. That tradeoff was
not acceptable for registration writes, so the final implementation uses a
bounded `sync.RWMutex` map. It still avoids the much larger transient heap and
retained-map cost of copy-on-write.

## Verification

```text
make format-ch020-registry
make test-ch020-registry
make test-ch020-package
make race-ch020-registry
make vet-ch020-registry
make benchmark-ch020-registry
```
