# M-U05 Arrangement-Only Recovery

M-U05 adds public checkpoints for maintained typed-table aggregate, join, and
sorted arrangements. A process can persist the arrangement state, restart,
validate the source versions, and resume from the checkpoint without rereading
the source changefeed.

## Scope

Aggregate checkpoints retain grouped values, count, sum, min/max value counts,
and distinct-value counts. Join checkpoints retain both input row maps and
rebuild the join indexes and pair set during restore. Sorted checkpoints retain
detached rows and the complete scalar or composite `ORDER BY` definition;
restore rebuilds the ordered vector and any configured string dictionaries.
Checkpoints are detached values and can be encoded with `encoding/json` or
another application-owned format.

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

For a sorted arrangement, `CaptureCheckpoint` and
`NewTypedTableSortedArrangementFromCheckpoint` provide the same detached
recovery path. The latter validates the target table's schema and source
sequence but does not scan its rows:

```go
checkpoint, err := sorted.CaptureCheckpoint()
if err != nil {
	return err
}
restored, err := hatSql.NewTypedTableSortedArrangementFromCheckpoint(table, checkpoint)
if err != nil {
	return err
}
_ = restored.RowsPage(0, 100)
```

The registry methods capture or restore all currently maintained definitions:

- `TypedTableAggregateArrangements.CaptureCheckpoints`
- `TypedTableAggregateArrangements.RestoreCheckpoints`
- `TypedTableJoinArrangements.CaptureCheckpoints`
- `TypedTableJoinArrangements.RestoreCheckpoints`
- `TypedTableSortedArrangement.CaptureCheckpoint`
- `TypedTableSortedArrangement.RestoreCheckpoint`
- `NewTypedTableSortedArrangementFromCheckpoint`

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
are bounded as well. Sorted checkpoints reject empty or duplicate row keys,
wrong column counts, invalid ordered-field kinds, and more than
`MaxTypedTableArrangementCheckpointRows` rows. Restore constructs a private
candidate state before replacing live rows, so malformed input leaves the
existing arrangement unchanged.

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

The sorted-arrangement comparison used five samples with
`-benchtime=100ms` on the same host. The baseline is clean commit `c84021f2`;
the feature run includes the checkpoint implementation. Existing `ORDER BY`
maintenance stayed within normal run-to-run variance. The new path is a
checkpoint/control-plane operation, not a per-row optimization.

| Workload | Baseline median | Feature median | Feature B/op | Feature allocs/op | Relative CPU |
| --- | ---: | ---: | ---: | ---: | ---: |
| Sorted arrangement build, legacy scalar | 4,919,906 ns | 4,986,523 ns | 2,163,448 | 8,267 | 1.01x |
| Sorted arrangement apply, 256-row batch | 768,767 ns | 688,350 ns | 407,020 | 278 | 0.90x |
| Sorted arrangement rows page | 919.1 ns | 858.9 ns | 1,376 | 11 | 0.93x |
| Capture checkpoint, 1,024 rows | N/A | 218,840 ns | 139,385 | 1,028 | N/A |
| Restore checkpoint, 1,024 rows | N/A | 871,450 ns | 302,757 | 1,042 | N/A |

Capture and restore allocations are proportional to detached checkpoint state
and are intentionally outside the row-update path. The sorted arrangement also
retains one table pointer for source-version validation; existing hot-path
allocation counts did not change in the measured suite.

Raw feature samples (ns/op) were capture `218840; 219458; 217682; 222656;
217947` and restore `871450; 879184; 856419; 870437; 877141`.

## Verification

```sh
make test-m055
make verify-m055
```

Tests cover aggregate and join JSON round-trips, exact update continuation,
global aggregates, source-version fencing, malformed counted state, duplicate
definitions, and atomic failure cleanup.
