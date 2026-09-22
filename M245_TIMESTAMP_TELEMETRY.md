# M245 Timestamp Throughput And Input-to-Output Latency

M245 adds an explicit, opt-in observation API for streaming adapters that
already know a logical input and output timestamp. It follows Materialize's
frontier and freshness model while keeping the core query executor free of
timestamp bookkeeping.

```go
telemetry.ObserveSQLTimestamp(hatSql.SQLTimestampTelemetryEvent{
    InputTimestamp:  sourceFrontier,
    OutputTimestamp: outputFrontier,
    Updates:         updates,
    Batches:         batches,
    InputAtUnixNano:  inputReceivedAt,
    OutputAtUnixNano: outputEmittedAt,
})
```

`TimestampUpdatesTotal` and `TimestampBatchesTotal` are cumulative counters;
Prometheus or another metrics backend can derive updates-per-second and
batches-per-second with its normal counter-rate functions. The latest logical
input and output timestamps are monotone gauges. A freshness sample is counted
only when both wall-clock values are positive and output is not earlier than
input, so malformed or unavailable timing data cannot underflow or create a
false latency.

## Exported Metrics

Prometheus names:

- `hatrie_sql_timestamp_observations_total`
- `hatrie_sql_timestamp_updates_total`
- `hatrie_sql_timestamp_batches_total`
- `hatrie_sql_timestamp_input`
- `hatrie_sql_timestamp_output`
- `hatrie_sql_input_to_output_latency_seconds` histogram

OpenTelemetry-compatible names mirror these fields under the
`hatrie.sql.timestamp` namespace. The timestamp observer does not run unless a
caller invokes `ObserveSQLTimestamp`; normal query execution and the existing
query observer payload remain unchanged.

## Measurement

Five `-benchmem` samples were collected on Linux amd64, AMD Ryzen 9 5950X.
The parent/current comparison uses the existing query observer benchmark;
the timestamp row measures the new opt-in observation path.

| Path | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| M244 parent query observer | 10.13 | 0 | 0 | baseline |
| M245 current query observer | 10.30 | 0 | 0 | 1.02x time; within noise |
| M245 timestamp observer | 9.453 | 0 | 0 | opt-in path; allocation-free |

Raw samples:

```text
parent query: 10.13 9.920 9.891 10.54 10.67 ns/op; 0 B/op; 0 allocs/op
current query: 10.30 10.95 10.59 9.845 10.05 ns/op; 0 B/op; 0 allocs/op
timestamp: 11.02 10.40 9.226 8.739 9.453 ns/op; 0 B/op; 0 allocs/op
```

The current query-observer median is 0.17 ns/op higher, or about 1.7%, with
unchanged allocations. That difference is below the useful precision of this
short lock-and-counter benchmark. The new path adds no allocation and is
entirely caller-driven.

## Verification

```text
make test-m245
make benchmark-m245
make race-m245
make vet-m245
```

The focused M245 and existing SQL telemetry tests pass. The broader package
check remains subject to the existing checkpoint-recovery failures documented
in M244's verification notes.
