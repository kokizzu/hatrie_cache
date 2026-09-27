# C154g durable rolling-schema runner

C154g adds an opt-in checkpoint-aware runner to `hatSchema.RollingSchemaPlan`:

```go
persist := hatSchema.RollingSchemaCheckpointPersistFunc(
	func(ctx context.Context, checkpoint hatSchema.RollingSchemaCheckpoint) error {
		wire, err := checkpoint.MarshalBinary()
		if err != nil {
			return err
		}
		return checkpointStore.Save(ctx, wire)
	},
)

err := plan.RunWithCheckpoint(ctx, deployment, persist, install, activate)
```

The callback receives a detached checkpoint after every successful stable phase:
`prepared` after installation and `active` after activation. A persistence error
is returned after the in-memory phase has completed. On restart, restore the
last successfully saved checkpoint with `plan.Restore` and retry the runner.
The unfinished hook may run again, so install and activation hooks must be
idempotent, as they already are for the existing retryable `Run` API.

The callback owns durable storage, fsync, encryption, authentication, and
retention. `RollingSchemaCheckpoint.MarshalBinary` provides bounded deterministic
encoding and CRC protection, but CRC is not an authentication mechanism.
Transport hooks and the exact replication schema gate remain caller-owned.

## Defaults and compatibility

`RollingSchemaPlan.Run` is unchanged and passes no persistence callback. Existing
callers, schemas, replica ordering, phase transitions, and retry behavior keep
their previous path. `RunWithCheckpoint` rejects a nil persistence callback and
uses the same validation and synchronized deployment state machine.

## Measurement

Command:

```text
make benchmark-c154g-rollout
```

Fixture: four replicas, a two-column compatible schema transition, no-op
install/activate hooks, five benchmark samples on Linux/amd64 (AMD Ryzen 9
5950X). The pre-change control was measured before the implementation; the
post-change control confirms the legacy path.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative to post-change legacy |
| --- | ---: | ---: | ---: | ---: |
| Legacy `Run`, pre-change | 3,564 | 7,636 | 30 | 1.05x time baseline |
| Legacy `Run`, post-change | 3,377 | 7,636 | 30 | 1.00x |
| `RunWithCheckpoint`, no-op persistence callback | 4,316 | 8,404 | 38 | 1.28x time, 1.10x heap, 1.27x allocs |

Raw samples:

```text
pre-change legacy: 3564, 3463, 3461, 3601, 3691 ns/op; 7636 B/op; 30 allocs/op
post-change legacy: 3379, 3377, 3489, 3278, 3312 ns/op; 7636 B/op; 30 allocs/op
checkpoint runner: 4327, 4344, 4315, 4316, 4288 ns/op; 8404 B/op; 38 allocs/op
```

The legacy control stayed at the same allocation profile; its small timing
variation is benchmark noise. The durable runner's overhead is the explicit
tradeoff for retaining a restart point after each stable phase. It is not
enabled by default and should be used when recovery progress is worth that
cost.

## Verification

Focused tests cover stable-phase ordering, detached checkpoints, persistence
failure, restore from the last durable checkpoint, idempotent retry behavior,
and nil callback validation:

```text
make format-c154g-rollout
make test-c154g-rollout
make benchmark-c154g-rollout
```
