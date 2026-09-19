# M-U05 Arrangement-Only Recovery

M-U05 adds public checkpoints for maintained typed-table aggregate and join
arrangements. A process can persist the arrangement state, restart, validate
the source versions, and resume from the checkpoint without rereading the
source changefeed.

## Scope

Aggregate checkpoints retain grouped values, count, sum, min/max value counts,
and distinct-value counts. Join checkpoints retain both input row maps and
rebuild the join indexes and pair set during restore. Checkpoints are detached
values and can be encoded with `encoding/json` or another application-owned
format.

Arrangement-only recovery is exact only when every source sequence in the
checkpoint still equals the current source sequence. If a source advanced,
restore returns `ErrTypedTableArrangementSourceVersionMismatch`; the caller
must hydrate from retained changes or rebuild from a source snapshot.

## API

```go
checkpoint, err := arrangement.CaptureCheckpoint()
if err != nil {
	return err
}

// Persist checkpoint with the application's durable snapshot/WAL protocol.

restored, err := arrangements.RestoreCheckpoints([]hatSql.TypedTableAggregateArrangementCheckpoint{
	checkpoint,
})
if err != nil {
	return err
}
defer restored[0].Release()
```

The registry methods capture or restore all currently maintained definitions:

- `TypedTableAggregateArrangements.CaptureCheckpoints`
- `TypedTableAggregateArrangements.RestoreCheckpoints`
- `TypedTableJoinArrangements.CaptureCheckpoints`
- `TypedTableJoinArrangements.RestoreCheckpoints`

Restoration is atomic at the registry-call level. Duplicate definitions,
existing definitions, invalid checkpoints, and source-version mismatches leave
newly created leases released. A single-arrangement restore replaces its
state only after validation and reconstruction succeed.

## Validation And Security

The restore path validates the checkpoint version, table identities, exact
arrangement definition, source sequence, checkpoint sequence, typed values,
duplicate keys, counted-value bounds, and row/group limits. Aggregate value
counts cannot exceed their group's row count. Join checkpoints are bounded by
`MaxTypedTableArrangementCheckpointRows`; aggregate groups and counted values
are bounded as well.

The checkpoint API does not authenticate or durably order data. Applications
must authenticate persisted checkpoint input and commit it with the same
ordering rules as their source offsets/WAL. A checkpoint should not be
accepted as a source snapshot merely because its JSON decoded successfully.

## Measurements

Command:

```sh
make benchmark-m055
```

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X. The fixture has
10,000 rows, 32 groups, sum/min/max, and distinct-name state. The replay row
is a same-fixture control that creates a new aggregate and applies all 10,000
changes. Values below are medians from the raw samples.

| Workload | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: |
| Capture checkpoint | 4,360,163 | 2,906,705 | 517 | 0.72x replay |
| Restore checkpoint | 4,233,413 | 2,646,024 | 496 | 0.70x replay |
| Rebuild by replaying 10,000 changes | 6,015,176 | 3,137,138 | 1,035 | 1.00x |
| JSON decode of checkpoint | 40,715,309 | 4,778,789 | 5,143 | 6.77x replay |
| JSON marshal plus decode | 45,850,842 | 12,661,842 | 5,162 | 7.62x replay |

Raw samples are intentionally kept in the benchmark output. Host scheduling
variance was visible in this short run, so repeat `make benchmark-m055` on the
deployment host before selecting a checkpoint cadence. The result is a
recovery/control-plane win for this full aggregate state, not a claim that
JSON persistence belongs on a row-update or query hot path.

## Verification

```sh
make test-m055
make verify-m055
```

Tests cover aggregate and join JSON round-trips, exact update continuation,
global aggregates, source-version fencing, malformed counted state, duplicate
definitions, and atomic failure cleanup.
