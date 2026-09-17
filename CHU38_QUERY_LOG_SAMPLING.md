# CH-U38 Sampled Query-Log Export

`SQLQueryLog` can retain a deterministic Bernoulli sample of valid,
privacy-safe terminal query records. The feature is opt-in through
`SQLQueryLogOptions`; existing callers and the `SQLQueryManager` default do
not change.

```go
log, err := hatSql.OpenSQLQueryLogWithOptions("var/query.ndjson", hatSql.SQLQueryLogOptions{
	SampleRate: 0.25,
	SampleSeed: 17,
})
if err != nil {
	return err
}
defer log.Close()

// The manager writes terminal statuses to QueryLog when it is configured.
stats := log.SamplingStats()
fmt.Println(stats.Accepted, stats.Dropped)
```

## Configuration

- `SampleRate: 0` preserves the existing retain-all behavior.
- `SampleRate: 1` explicitly retains every valid entry.
- `0 < SampleRate < 1` enables deterministic sampling for the append order.
- `SampleSeed: 0` selects a stable built-in seed. A nonzero seed makes the
  selection reproducible across log instances that receive the same entries in
  the same order.
- `SamplingStats` reports the effective rate and `Observed`, `Accepted`, and
  `Dropped` counters since the log was opened. The counters are process-local
  and are not persisted in the NDJSON file. `Accepted` means selected for
  writing; a later filesystem error can still prevent persistence.

Malformed or oversized records still return the existing validation error and
are not counted as observed. A valid dropped record does not trigger rotation,
does not write a line, and does not call `Sync`.

The sampler intentionally runs after validation, size checking, and JSON
encoding. This preserves the existing error contract and keeps accepted writes
ordered under the existing log mutex. Sampling therefore reduces durable file
I/O, retained records, and retained log bytes, but it does not eliminate the
CPU or temporary allocation needed to encode a candidate record. The log still
contains only the existing privacy-safe fields: query ID, terminal state,
timestamps, duration, and error code. The seed is a reproducibility control,
not a secret or an access-control mechanism.

## Verification

Focused tests are run with:

```text
make test-chu38
make test-chu38-package
make race-chu38
make vet-chu38
```

The implementation is in `hat/hatSql/query_log.go`; the regression tests and
benchmark are in `hat/hatSql/chu38_query_log_sampling_test.go`.
