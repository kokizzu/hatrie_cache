# CH-007 TTL Scheduler And Durable Deadlines

`hatSql.TypedTable` already supports processing-time and event-time row TTL.
This extension adds two explicit maintenance operations:

- `TypedTableTTLScheduler` runs one shared bounded reaper for registered tables.
- `MarshalTTLState` and `RestoreTTLState` persist processing-time deadlines
  separately from row data.

The scheduler does not start automatically, and it does not create one
goroutine per table. It calls the existing indexed `PurgeExpired` method, which
publishes ordinary `DELETE` changes and preserves the current visibility and
changefeed semantics. Column-level TTL, automatic schema integration, and
retry policy remain caller-owned.

## Scheduler Usage

```go
scheduler, err := hatSql.NewTypedTableTTLScheduler(
	hatSql.TypedTableTTLSchedulerOptions{
		PollInterval:      time.Second,
		MaxTablesPerCycle: 64,
		OnRun: func(run hatSql.TypedTableTTLRun) {
			log.Printf("TTL table=%s expired=%d error=%s", run.Name, run.Expired, run.Error)
		},
	},
)
if err != nil {
	return err
}
if err := scheduler.Register("events", events); err != nil {
	return err
}
if err := scheduler.Start(ctx); err != nil {
	return err
}
defer scheduler.Close()
```

The zero-value options use a one-second poll interval and a 64-table cycle
bound. `Start` is the opt-in point; construction and registration have no
background activity. `RunOnce(ctx)` is available for deterministic operators,
tests, or applications that already own a maintenance loop. `Status` and
`Snapshot` expose the latest run for each registered table. A run records its
name, start/finish timestamps, expired-row count, and error text.

The scheduler uses one goroutine regardless of the number of registered tables.
`MaxTablesPerCycle` bounds work per pass; tables are selected in deterministic
name order. `Close` is idempotent and waits for a current purge pass to finish.
The scheduler does not expose a network endpoint or execute untrusted code.
Any administrative endpoint must apply authentication and authorization before
registering tables or exposing status.

## Deadline Snapshot

`MarshalTTLState` writes a deterministic `HTTL1` little-endian snapshot with:

- the table name, processing-time mode, and configured lifetime;
- one key and one absolute Unix-nanosecond deadline for every physical row;
- a CRC-32 checksum over the preceding bytes.

The snapshot is bounded to 16 MiB. It contains no row values, journal entries,
or authentication tag, so it is a state supplement rather than a backup and
must be stored with the normal protected backup. CRC detects accidental
corruption; callers must provide confidentiality and authenticity when the
keys or retention policy are sensitive.

Restore row data first, then call `RestoreTTLState`, then start the scheduler:

```go
state, err := table.MarshalTTLState()
if err != nil {
	return err
}
// Persist state beside the table snapshot, then on recovery:
if err := table.RestoreTTLState(state); err != nil {
	return err
}
```

Restore validates the magic, version, mode, lifetime, size, checksum, exact
table name, exact physical row count, and complete key set before changing the
table. Record order may differ. A corrupt, mismatched, duplicated, truncated,
or oversized snapshot leaves the current deadlines unchanged. Event-time TTL
is derived from its source column and therefore rejects this snapshot format.

Processing-time deadline state is only an expiry supplement. A full backup
must still include the row data, schema, journal/checkpoint, and the ordering
rules documented by the backup subsystem. Restore rehearsal should verify both
the table rows and TTL behavior at the deadline boundary.

## Measurement

Reproduce the clean-baseline and current runs with
`make benchmark-before-ch007-scheduler-c282` and
`make benchmark-ch007-scheduler-c282`. Both use `GOMAXPROCS=1`, five 200-ms
samples, Linux/amd64, and an AMD Ryzen 9 5950X.

| Operation | Median | B/op | Allocs/op | Relative cost |
|---|---:|---:|---:|---:|
| Existing direct no-op `PurgeExpired` | 11.61 ns/op | 0 | 0 | 1.00x |
| Scheduler `RunOnce`, one registered table | 186.3 ns/op | 96 | 1 | 16.0x slower |
| `MarshalTTLState`, 4,096 rows | 38,828 ns/op | 81,920 | 1 | Explicit snapshot cost |

The scheduler overhead is maintenance orchestration: table selection, status
creation, and observer handling. It is not charged to normal reads, writes, or
manual purge callers. The feature is kept opt-in because its value is reliable
expiry execution and operator visibility, not a hot-path speedup.

Focused lifecycle, cancellation, snapshot-corruption, and expiry-boundary
tests are in `hat/hatSql/ch007_ttl_scheduler_test.go`. Run
`make verify-ch007-scheduler-c282` for normal tests, the race detector, and vet.
