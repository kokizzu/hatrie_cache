# Replication Metrics

The monitoring Prometheus endpoint exposes asynchronous replication queue health
and outgoing HTTP request wire usage. These metrics are additive and do not
change the default protobuf wire format, gzip threshold, batching, retry policy,
or request lifecycle.

## Queue Metrics

When asynchronous replication is configured, `/metrics` includes:

- `hatrie_cache_replication_queue_depth` and `hatrie_cache_replication_queue_capacity`;
- `hatrie_cache_replication_queue_enqueued_total` and `hatrie_cache_replication_queue_dropped_total`;
- `hatrie_cache_replication_attempts_total`, `hatrie_cache_replication_successes_total`, `hatrie_cache_replication_failures_total`, and `hatrie_cache_replication_retried_total`;
- `hatrie_cache_replication_queue_oldest_age_millis`, `hatrie_cache_replication_in_flight_age_millis`, and `hatrie_cache_replication_last_retry_age_millis`.

## Wire Metrics

The following counters have `node`, `target`, and `encoding` labels:

- `hatrie_cache_replication_request_wire_bytes_total` counts bytes read from outgoing request bodies;
- `hatrie_cache_replication_request_wire_requests_total` counts outgoing request bodies.

The `encoding` label is `identity` when `Content-Encoding` is absent and
`gzip` when thresholded request compression is active. Counts are grouped by
target, so a retry or a typed-payload compatibility fallback is counted as its
own attempted request. A partially transmitted failed request contributes the
bytes that were read before failure.

These are request-body bytes, not HTTP headers or response bytes. They measure
compressed bytes when gzip is active. Compare `gzip` and `identity` rates to
estimate transport savings; exact compression ratio is intentionally not
reported because streamed JSON/protobuf bodies need not materialize an
uncompressed copy.

Example Prometheus queries:

```promql
sum by (target, encoding) (rate(hatrie_cache_replication_request_wire_bytes_total[5m]))
```

```promql
sum by (encoding) (rate(hatrie_cache_replication_request_wire_requests_total[5m]))
```

The public Go snapshot is available from
`HTTPReplicator.MetricsSnapshot()` through `TargetWireBytes` and
`TargetWireRequests`, each keyed by target and encoding. Snapshots are deep
copies and can be inspected without holding the replicator lock.

## Cost

The instrumentation adds one reader wrapper per outgoing HTTP request and one
counter update after the request completes. It does not allocate per payload
chunk and uses the existing metrics mutex only after transport completion. The
focused benchmark target records the small counter/snapshot cost; the feature
is observability-only, so it has no expected latency or bandwidth improvement.
