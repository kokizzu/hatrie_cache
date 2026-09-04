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
```

Use pause during a planned peer outage or maintenance window, monitor queue depth and durable backlog, then resume and verify that the backlog drains. Pause is not a data-loss operation; closing the process or exhausting configured non-durable queue capacity still follows the existing queue semantics.
