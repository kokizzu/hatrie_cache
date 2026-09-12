# Durable Frontier Snapshots

`hatPipeline.FrontierRegistry` keeps named lower/upper frontiers in memory.
`MarshalSnapshot` and `RestoreSnapshot` provide a versioned compact binary
checkpoint so an owner can persist and restore that control-plane state during
restart or backup.

## Save And Restore

```go
frontiers, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
if err != nil {
	return err
}
if err := frontiers.Register("orders-apac"); err != nil {
	return err
}
if err := frontiers.Advance("orders-apac", 120, 128); err != nil {
	return err
}

checkpoint, err := frontiers.MarshalSnapshot()
if err != nil {
	return err
}
// Persist checkpoint with the application's backup/WAL metadata.

restored, err := hatPipeline.NewFrontierRegistry(hatPipeline.FrontierRegistryOptions{})
if err != nil {
	return err
}
if err := restored.RestoreSnapshot(checkpoint); err != nil {
	return err
}
```

`MarshalSnapshot` is deterministic: records are sorted by ID and include the
ID, lower frontier, upper frontier, generation, and UTC update timestamp.
`RestoreSnapshot` parses and validates all records before changing the target.
The target must be empty; it never overwrites already-registered frontiers.
Restored values continue to obey monotone `Advance` checks.

The format begins with the `HFR1` magic and version `1`, then uses unsigned
varints for counts and frontier values and a signed varint for Unix nanoseconds.
The current limits are 64 MiB per snapshot, 1,048,576 frontier records, and
1 MiB per frontier ID. Registry object limits still apply during restore.
Malformed data, duplicate IDs, `lower > upper`, unknown versions, trailing
bytes, and capacity overflow are rejected with no partial mutation.

This is a serialization contract, not automatic persistence. The caller must
persist the returned bytes, coordinate them with its command journal or source
offset, and decide the crash/recovery ordering. No file is opened, no secret is
embedded, and no background worker is created by these methods.

## Measurement

On an AMD Ryzen 9 5950X with 128 named frontiers, `make
benchmark-m-u09-snapshot` measured:

| Operation | Binary snapshot | JSON control |
|---|---:|---:|
| Encode | 18.891-19.600 us/op, 17,208 B/op, 5 allocs | 57.707-57.763 us/op, 19,802-19,808 B/op, 130 allocs |
| Decode | 10.260-10.338 us/op, 18,088 B/op, 132 allocs | 155.270-156.502 us/op, 21,312 B/op, 142 allocs |
| Serialized size | 2,955 bytes | 13,101 bytes |

Binary encoding is about `3x` faster, `4.43x` smaller for transfer/storage, and
uses `26x` fewer allocations than JSON. Binary decoding is about `15x` faster;
its allocation count is closer because both paths construct 128 decoded
records. The benchmark compares serialization parsing and does not claim that
frontier snapshots replace a complete source/WAL-consistent backup.

Focused checks:

```sh
make test-m-u09-snapshot
make test-m-u09-frontier
make race-m-u09-snapshot
make vet-m-u09-snapshot
```
