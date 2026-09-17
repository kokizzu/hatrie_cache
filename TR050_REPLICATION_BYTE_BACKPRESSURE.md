# TR-050 Replication Byte Backpressure

## Problem

The asynchronous replication queue already limits the number of jobs, but job
size can vary substantially. A small number of large commands can therefore
hold much more memory than a slot count suggests.

## Design

`HTTPReplicatorOptions.AsyncQueueMaxBytes` adds an opt-in estimated resident-byte
budget. The admission check counts both `EstimatedQueuedBytes` and
`EstimatedInFlightBytes`, using the existing per-command resident-size
estimator. The check and pending-metadata update happen under the same mutex, so
concurrent producers cannot oversubscribe the configured budget.

The default is `0` and remains disabled. With the budget disabled, the old
slot-only admission path is retained. With a positive budget, a non-durable job
that does not fit is skipped and counted as dropped. A journal-backed job that
does not fit stays in the durable outbox backlog and causes refill to retry it
later.

The configured limit is exposed as `max_bytes` in queue stats and as
`hatrie_cache_replication_queue_max_bytes` in Prometheus. The estimate is an
operational guard for replication-resident data; it does not include every
allocator or runtime overhead and is not a process-wide memory limit.

## Configuration

```go
replicator := NewHTTPReplicator(HTTPReplicatorOptions{
	AsyncQueueSize:     1024,
	AsyncQueueMaxBytes: 64 << 20,
	AsyncOutbox:        outbox,
})
```

Use a durable `AsyncOutbox` when replication loss is unacceptable. Without a
journal or outbox, the existing bounded-queue contract applies: foreground
commands receive an explicit skipped result when the asynchronous queue cannot
admit the replication job.

## Verification

The focused regression suite covers default-off behavior, configured stats,
queued plus in-flight accounting, non-durable rejection, and durable backlog
retention:

```text
make test-tr050-replication-byte-backpressure
```

The package verification is:

```text
make test-tr050-replication-package
```

Benchmark samples and the raw output are in
[BENCHMARK.md](BENCHMARK.md#tr-050-rate-aware-replication-byte-backpressure).
