# Replication Operations

## Pause And Resume

Asynchronous replication is enabled only when `HTTPReplicatorOptions.AsyncQueueSize` is greater than zero. It starts unpaused by default, so existing deployments keep their current behavior.

Pausing stops the async worker from starting queued jobs. A request already in flight is allowed to finish. New jobs are still admitted using the existing queue and outbox rules. Durable journal-backed jobs remain available and are delivered after resume.

```go
replicator := hatCache.NewHTTPReplicator(hatCache.HTTPReplicatorOptions{
	AsyncQueueSize: 1024,
})

if err := replicator.PauseAsyncReplication(); err != nil {
	// The async queue was not configured or the replicator was closed.
}

if err := replicator.ResumeAsyncReplication(); err != nil {
	// The async queue was not configured or the replicator was closed.
}
```

The methods are idempotent. `AsyncReplicationPaused` and `ReplicationResult.Queue.Paused` expose the current state. Calling either method on a replicator without an async queue returns `ErrAsyncReplicationDisabled`.

## Monitoring API

The existing authenticated `POST /api/replication` endpoint accepts these additive actions:

```json
{"action":"pause"}
```

```json
{"action":"resume"}
```

Both return the normal replication status payload. A POST without `action` keeps the existing manual sync behavior and accepts the existing `prefix` field. Unknown actions return HTTP 400. The dangerous-action protection, authentication, audit logging, and rate limiting still apply.

The `/metrics` endpoint exposes:

```text
hatrie_cache_replication_queue_paused{node="..."} 0|1
hatrie_cache_replication_queue_estimated_queued_bytes{node="..."} N
hatrie_cache_replication_queue_estimated_in_flight_bytes{node="..."} N
hatrie_cache_replication_queue_wait_millis_bucket{node="...",le="..."} N
hatrie_cache_replication_queue_service_millis_bucket{node="...",le="..."} N
```

The byte gauges are bounded estimates of payload and replication metadata held by
the in-memory queue or current delivery. They exclude durable-only outbox
backlog, Go allocator overhead, and process-wide RSS. Queue wait includes time
from enqueue until delivery starts; service covers one async job, including its
retry handling. Use the gauges with queue depth and the histograms with target
latency to distinguish payload pressure from a slow peer.

Use pause during a planned peer outage or maintenance window, monitor queue depth and durable backlog, then resume and verify that the backlog drains. Pause is not a data-loss operation; closing the process or exhausting configured non-durable queue capacity still follows the existing queue semantics.
