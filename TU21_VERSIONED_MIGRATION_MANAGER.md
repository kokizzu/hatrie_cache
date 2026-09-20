# T-U21 Versioned Migration Manager

`VersionedMigrationManager` is an opt-in coordination layer for online schema
migrations. It stores named plans, dependency and precondition acknowledgements,
mixed-version client capabilities, resumable progress, pause/resume state, and
rollback state. It does not transform application rows; the caller performs
the work and reports completed units.

## Lifecycle

1. Register a `MigrationPlan` with source/target versions and a positive work
   estimate.
2. Register every known client with the inclusive version range it supports.
3. Acknowledge each plan precondition, such as a verified backup.
4. Call `Start`. Dependencies must be completed and every registered client
   must support both the source and target versions.
5. Report work with `Advance`; use `Pause` and `Resume` around operator or
   deployment interruptions.
6. `Rollback` clears progress and acknowledgements. It is rejected while a
   dependent migration has started or completed.

An active migration cannot unregister a client capability. This prevents a
caller from bypassing the mixed-version gate by deleting the record during a
transition. Client membership and liveness still need to be maintained by the
caller.

## Snapshot and recovery

`MarshalBinary` emits a bounded `HTM1` envelope containing JSON coordination
metadata and a CRC32C checksum. `UnmarshalBinary` validates the complete
payload, dependencies, lifecycle invariants, client compatibility, and
checksum before atomically replacing manager state. The encoded snapshot is
limited to 16 MiB.

The manager does not choose a filesystem, fsync policy, or replication policy.
Callers should write snapshots through their existing atomic durable-storage
path and keep the last known-good snapshot until the replacement is verified.

## Example

```go
manager := hatDataStructure.NewVersionedMigrationManager()
_ = manager.RegisterClient(hatDataStructure.MigrationClient{
	Name: "api-1",
	MinVersion: 1,
	MaxVersion: 2,
})
_ = manager.RegisterPlan(hatDataStructure.MigrationPlan{
	Name: "users-v2",
	FromVersion: 1,
	ToVersion: 2,
	TotalUnits: 100000,
	Preconditions: []string{"backup-complete"},
})
_ = manager.AcknowledgePrecondition("users-v2", "backup-complete")
_ = manager.Start("users-v2")
_ = manager.Advance("users-v2", 1000)
```

## Measured cost

Measured on Linux/amd64, AMD Ryzen 9 5950X, with five `-benchmem` samples:

| Operation | Median | Memory |
| --- | ---: | ---: |
| `Status` | 46.33 ns/op | 0 B/op, 0 allocs/op |
| `Advance` | 14.06 ns/op | 0 B/op, 0 allocs/op |
| `MarshalBinary` | 863.0 ns/op | 528 B/op, 4 allocs/op |
| `UnmarshalBinary` | 4.261 us/op | 1,952 B/op, 25 allocs/op |

The lifecycle path is cheap enough for control-plane updates. Snapshot JSON
and validation intentionally cost more because recovery correctness and
corruption detection matter more than serializing a handful of metadata
records. Reproduce with `make benchmark-tu21` and `make test-tu21`.
