# CH-U23 Async Insert Queue Status

Hatrie Cache now provides an importable, bounded registry for caller-owned
`AsyncInsertBuffer` instances. It adds queue depth counters and authenticated
operator endpoints without enabling a worker or monitoring route by default.

## API

```go
registry, err := hatriecache.NewAsyncInsertQueueRegistry(0)
if err != nil {
	return err
}
if err := registry.Register("events", buffer); err != nil {
	return err
}

handler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
	AuthToken:         os.Getenv("HATRIE_AUTH_TOKEN"),
	AsyncInsertQueues: registry,
})
```

`NewAsyncInsertQueueRegistry(0)` uses a 16-queue default and accepts at most
1024 queues. Names are trimmed, must be non-empty, and are limited to 256
bytes. Registration is explicit, duplicate-safe, and does not own the
buffer's `Close` lifecycle. `Unregister` also leaves the buffer running.

`AsyncInsertBuffer.Stats` and registry snapshots report capacity, batch size,
queued items, in-flight items, pending items, cumulative submissions and
flushes, failures, and closed state. They never return command keys or values.

## Monitoring

Supplying `MonitoringOptions.AsyncInsertQueues` enables these authenticated
routes:

```text
GET  /api/async-inserts
POST /api/async-inserts/flush?name=events
POST /api/async-inserts/flush
```

The first route returns deterministic name-sorted snapshots. The second
flushes one named queue; omitting `name` flushes all registered queues in name
order. The flush response includes post-flush snapshots. Unknown names return
`404`; invalid names return `400`; canceled or timed-out flushes return `408`.
Existing monitoring authentication protects both routes because they are
under `/api/`. No queue route or OpenAPI path is registered when the option is
nil.

The registry does not create a daemon, replication topology, persistence
format, or cross-process queue. The caller still creates and closes the
buffers and journals. The queue status is operational telemetry, not a
durability acknowledgment; callers that need durability wait on the returned
`AsyncInsertSubmission`.

## Measurement

The benchmark uses one CPU, a 64-command batch, a 128-command buffer, five
runs, and `-benchmem`. Raw results are recorded in
[BENCHMARK.md](BENCHMARK.md#ch-u23-async-insert-queue-status-and-flush).

The submit path remained at 6 allocations and about 1.4 KiB/op. The latest
clean baseline median was 13,382 ns/op; the feature median was 13,556 ns/op,
about 1.3% higher and within the run-to-run noise observed on this host. An
initial atomic-counter experiment was about 5% slower and was removed. Queue
status for one registered queue measured a 174.3 ns/op median, 160 B/op, and
3 allocations/op. The cost is paid only when callers request a snapshot; the
default path remains off.
