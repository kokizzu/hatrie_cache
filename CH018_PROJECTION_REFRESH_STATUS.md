# CH-018 Projection Refresh Status

`IncrementalProjectionRunner.Status` exposes a bounded process-local snapshot
for an enabled journal-driven materialized projection. It makes a stale or
failed projection visible without changing the existing refresh, checkpoint,
or replay contract.

## Default

`IncrementalProjectionRunnerOptions.Enabled` remains `false` unless the caller
sets it to `true`. A disabled runner reports `State: "disabled"`; calling
`Status` never enables refresh work. The status bookkeeping is only active on
the existing opt-in runner path.

## Usage

```go
runner, err := hatSql.NewIncrementalProjectionRunner(
	views,
	resolver,
	hatSql.QueryOptions{},
	hatSql.IncrementalProjectionRunnerOptions{
		Name:    "people",
		Enabled: true,
		// CheckpointStore: durableStore,
	},
)
if err != nil {
	return err
}

status := runner.Status()
if status.State == hatSql.ProjectionRefreshStateFailed {
	log.Printf("projection %s failed: %s", status.Name, status.LastError)
}
```

`Status` returns a value copy. Callers can serialize or retain it without
sharing mutable runner state.

## State And Fields

| Field | Meaning |
| --- | --- |
| `Name` | Normalized runner name. |
| `Enabled` | Whether the runner is configured to process changes. |
| `State` | `disabled`, `idle`, `healthy`, `lagging`, or `failed`. |
| `AppliedSequence` | Highest sequence whose refresh and optional checkpoint save succeeded. |
| `ObservedSequence` | Highest sequence supplied to `Apply` or `Rebuild`. |
| `Lag` | Saturation-safe `ObservedSequence - AppliedSequence`, or zero when applied is at or beyond observed. |
| `LastAttemptAt` | UTC time of the latest refresh or rebuild attempt that reached refresh/checkpoint handling. |
| `LastSuccessAt` | UTC time of the latest successful refresh and checkpoint boundary update. |
| `LastError` | Last bounded failure message; capped at 1,024 bytes. |
| `ConsecutiveFailures` | Saturating count since the last successful refresh. |

`idle` is the initial state of an enabled runner. A successful refresh with no
unapplied observed input is `healthy`. A source sequence observed beyond the
applied boundary is `lagging`. Any refresh validation, source refresh, or
checkpoint-save failure is `failed`; the old applied sequence remains intact.
A later successful `Apply` or `Rebuild` clears the error and failure count.

## Durability And Recovery

Status is diagnostic process memory, not part of `ProjectionCheckpointStore`.
After restart, `AppliedSequence` and `ObservedSequence` start from the durable
checkpoint when one is loaded, while timestamps, errors, and failure counts
reset. The caller should treat the durable checkpoint and materialized view as
the recovery authority.

The runner refreshes before saving its checkpoint. If refresh or checkpoint
saving fails, `Status` reports the failure and `Checkpoint()` remains at the
previous successful boundary. Replaying the batch or using `Rebuild` remains
the supported recovery path.

## Cost

The status snapshot copies a fixed-size value and performs no heap allocation
in the benchmark. It does add timestamp/state bookkeeping to successful and
failed runner attempts. See [BENCHMARK.md](BENCHMARK.md#ch-018-projection-refresh-lag-and-failure-state)
for the baseline and raw samples.
