# T209 Relay/Applier Backpressure

T209 adds an opt-in lag guard for asynchronous replication. The relay observes
the highest journal sequence acknowledged by downstream replicas and pauses
new asynchronous admission when the observed lag crosses a high watermark.
Admission resumes only after lag falls to a lower watermark. This hysteresis
avoids repeatedly opening and closing the relay around one boundary.

The idea is useful for Tarantool-style relay/applier pipelines and for
Materialize-style feedback-driven ingestion: downstream progress is part of
the flow-control signal instead of relying only on a local queue's item or byte
capacity.

## Configuration

The feature is disabled by default. Enable it explicitly on the asynchronous
replicator:

```go
replicator := hatCache.NewHTTPReplicator(hatCache.HTTPReplicatorOptions{
	AsyncQueueSize: 1024,
	AsyncRelayBackpressure: hatReplication.RelayBackpressureOptions{
		Enabled:         true,
		HighWatermark:   10_000,
		ResumeWatermark: 5_000,
	},
})
```

Watermarks are journal-entry counts, not time. Zero selects the defaults:
`10,000` entries to pause and `5,000` entries to resume. A resume watermark at
or above the high watermark is clamped below it.

The existing `AsyncQueueMaxBytes` limit remains independent and should still be
used to cap resident memory. T209 adds a progress guard; it does not replace
the byte budget.

## Behavior

- Before a new job is admitted, the controller computes the maximum known
  source-to-acknowledgement lag.
- Lag at or above `HighWatermark` changes the controller to `paused` and
  rejects non-durable asynchronous admission with reason
  `replication relay backpressure is active`.
- A journal-backed job is retained in the durable journal/outbox backlog with
  reason `replication relay backpressure is active; job retained in durable
  journal backlog`.
- Acknowledgements update lag and can automatically clear the pause at or below
  `ResumeWatermark`.
- Existing manual `PauseAsyncReplication` state is separate from automatic
  backpressure state.

The policy is source-wide: one known lagging replica pauses new asynchronous
work for the relay. This is conservative and preserves command ordering across
the existing shared queue. Per-target queues would reduce collateral
throttling, but would add ordering, retention, and recovery complexity and are
not part of T209.

The queue health snapshot exposes `backpressure_enabled`,
`backpressure_paused`, `backpressure_lag`, both watermarks, and the number of
state transitions. These fields are omitted or zero when the feature is not
enabled.

## Verification

Focused correctness checks:

```text
make test-t209
```

The tests cover disabled defaults, high/low watermark hysteresis, watermark
normalization, lagging admission, automatic resume, and queue statistics.

## Measurements

The baseline was captured before implementation with
`make benchmark-t209-before`. The feature benchmark was captured with
`make benchmark-t209` on Linux/amd64 using an AMD Ryzen 9 5950X.

| Path | Median | Memory | Relative result |
| --- | ---: | ---: | --- |
| Existing byte-admission control | 92.79 ns/op | 16 B/op; 1 alloc/op | Compatibility baseline |
| Standalone legacy lag comparison | 0.2865 ns/op | 0 B/op; 0 allocs/op | Compatibility baseline |
| Standalone `RelayBackpressure.Admit` | 7.635 ns/op | 0 B/op; 0 allocs/op | 26.6x the synthetic comparison; no allocation |
| Integrated legacy relay admission | 10.88 ns/op | 0 B/op; 0 allocs/op | Compatibility baseline |
| Integrated T209 admission | 128.1 ns/op | 0 B/op; 0 allocs/op | 11.8x slower admission check; no allocation |

The CPU cost is paid only when the feature is explicitly enabled, and the
default path is unchanged. An attempted optimization that looked like it
would remove a lock did not pass measurement: it raised standalone admission
to roughly 26-30 ns/op and integrated admission to roughly 120-138 ns/op, so
it was reverted.

### Raw Baseline Output

```text
BenchmarkTR050ReplicationByteBudgetAdmission/legacy-control-32  92.79 ns/op  348.0 estimated_job_bytes/op  16 B/op  1 allocs/op
BenchmarkTR050ReplicationByteBudgetAdmission/legacy-control-32  92.41 ns/op  348.0 estimated_job_bytes/op  16 B/op  1 allocs/op
BenchmarkTR050ReplicationByteBudgetAdmission/legacy-control-32  93.28 ns/op  348.0 estimated_job_bytes/op  16 B/op  1 allocs/op
BenchmarkTR050ReplicationByteBudgetAdmission/legacy-control-32  98.69 ns/op  348.0 estimated_job_bytes/op  16 B/op  1 allocs/op
BenchmarkTR050ReplicationByteBudgetAdmission/legacy-control-32  92.02 ns/op  348.0 estimated_job_bytes/op  16 B/op  1 allocs/op
```

### Raw T209 Output

```text
BenchmarkT209RelayBackpressureLegacyLagCheck-32  0.2899 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureLegacyLagCheck-32  0.2865 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureLegacyLagCheck-32  0.2752 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureLegacyLagCheck-32  0.2792 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureLegacyLagCheck-32  0.2967 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureAdmit-32  7.635 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureAdmit-32  7.846 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureAdmit-32  7.350 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureAdmit-32  7.556 ns/op  0 B/op  0 allocs/op
BenchmarkT209RelayBackpressureAdmit-32  8.371 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureLegacyAdmission-32  11.20 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureLegacyAdmission-32  10.88 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureLegacyAdmission-32  9.029 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureLegacyAdmission-32  9.500 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureLegacyAdmission-32  11.26 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureAdmission-32  133.5 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureAdmission-32  128.1 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureAdmission-32  129.0 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureAdmission-32  114.8 ns/op  0 B/op  0 allocs/op
BenchmarkT209ReplicationRelayBackpressureAdmission-32  116.8 ns/op  0 B/op  0 allocs/op
```
