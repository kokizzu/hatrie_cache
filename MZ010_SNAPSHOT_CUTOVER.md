# MZ010 Cross-Source Snapshot Cutover

MZ010 adds an opt-in, bounded coordinator for publishing one logical snapshot
only after every participating source has reached the same requested
timestamp. It is a control-plane primitive: it does not start goroutines,
perform I/O, move source data, or enable itself globally. A caller opts in by
constructing `hatPipeline.SnapshotCutoverCoordinator`.

## Lifecycle

```go
coordinator, err := hatPipeline.NewSnapshotCutoverCoordinator(
	hatPipeline.SnapshotCutoverOptions{},
)
if err != nil {
	return err
}

_, err = coordinator.Prepare(hatPipeline.SnapshotCutoverSpec{
	ID:        "orders-snapshot-42",
	Timestamp: 1000,
	Sources: []hatPipeline.SnapshotCutoverSource{
		{ID: "orders-eu", Generation: 12},
		{ID: "orders-us", Generation: 9},
	},
})
if err != nil {
	return err
}

progress, err := coordinator.AcknowledgeProgress(
	"orders-snapshot-42",
	hatPipeline.SnapshotCutoverAcknowledgement{
		SourceID:   "orders-eu",
		Generation: 12,
		Lower:      1000,
		Upper:      1042,
	},
)
if err != nil {
	return err
}
if progress.Ready {
	_, err = coordinator.Commit("orders-snapshot-42")
}
```

The normal sequence is:

1. `Prepare` records the target timestamp and the expected generation of every
   source.
2. Each source calls `AcknowledgeProgress` after its lower frontier covers the
   target. `Upper` is retained for diagnostics and must not regress.
3. The caller calls `Commit`. It succeeds only after every source has
   acknowledged the target and is idempotent after commitment.
4. The caller may call `Forget` after commit or abort to release the bounded
   cutover slot.

`Acknowledge` remains available when the caller needs a detached,
deterministically ordered `SnapshotCutoverStatus` after every acknowledgement.
Use `Status` or `Snapshot` for diagnostics. `AcknowledgeProgress` returns only
state and counts and does not allocate on the successful hot path.

## Safety Rules

- Source IDs are normalized, unique, and sorted in returned status values.
- Acknowledgements from a different source generation are rejected, fencing a
  stale connector instance after restart or reassignment.
- A lower frontier below the requested timestamp returns
  `ErrSnapshotCutoverNotReady`.
- Lower and upper frontiers are monotone per source; regressions are rejected.
- Committing a prepared cutover before all sources acknowledge returns
  `ErrSnapshotCutoverNotReady`.
- Cutovers are bounded to 64 retained plans and 256 sources by default. The
  configurable hard maximum is 65,536 for each bound.
- Aborted and committed cutovers are terminal. Only terminal cutovers can be
  forgotten.

The coordinator does not make a distributed commit decision by itself. The
caller must connect `Commit` to its publication or query-snapshot boundary and
must decide how to retry, replay, or abort a source that cannot advance.

## Cost And Measurement

The benchmark uses 16 sources on Linux/amd64 with an AMD Ryzen 9 5950X and
five samples per case. It compares the coordinator APIs with the equivalent
manual source-count loop. This is control-plane accounting, not a claim about
query throughput.

| Operation | Median ns/op | B/op | allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Manual source coverage loop | 15.61 | 0 | 0 | 1.00x |
| `AcknowledgeProgress` compact path | 75.44 | 0 | 0 | 4.83x slower |
| `Acknowledge` detached status | 503.1 | 1,088 | 2 | 32.23x slower |

The compact path pays for locking, source/generation validation, and indexed
state updates without heap allocation. The detached path additionally copies
the source and acknowledgement slices so callers cannot mutate coordinator
state. The full status cost is therefore intentional and should be reserved
for status consumers; connectors acknowledging frequent frontiers should use
`AcknowledgeProgress`.

Raw final samples (`ns/op`, `B/op`, `allocs/op`):

```text
coordinator-status: 521.6 1088 2
coordinator-status: 520.7 1088 2
coordinator-status: 483.9 1088 2
coordinator-status: 495.0 1088 2
coordinator-status: 503.1 1088 2
coordinator-progress: 71.86 0 0
coordinator-progress: 75.37 0 0
coordinator-progress: 75.65 0 0
coordinator-progress: 78.07 0 0
coordinator-progress: 75.44 0 0
manual: 16.36 0 0
manual: 15.48 0 0
manual: 15.60 0 0
manual: 15.78 0 0
manual: 15.61 0 0
```

Run the benchmark with:

```text
make benchmark-mz010-snapshot-cutover
```

## Verification

Focused tests cover deterministic source ordering, missing and unknown
sources, generation fencing, readiness, monotonicity, terminal lifecycle,
capacity, detached results, and the allocation-free progress result. The
relevant checks are:

```text
make test-mz010-snapshot-cutover
make test-mz010-package
make race-mz010-snapshot-cutover
make vet-mz010-snapshot-cutover
```
