# Per-Space Operation Statistics

`hatMetrics.SpaceOperationMetrics` provides low-allocation counters for one
named space, table, shard, or index. `hatMetrics.SpaceOperationStatsRegistry`
keeps a bounded set of those handles and returns sorted snapshots for reporting.
The feature is opt-in: it does not add work to data-structure operations until
the caller records an event.

## Usage

```go
registry := hatMetrics.NewSpaceOperationStatsRegistry()

orders, err := registry.Register("orders")
if err != nil {
	return err
}

start := time.Now()
value, found := ordersCache.Get("order-42")
outcome := hatMetrics.SpaceOperationOutcomeMiss
if found {
	outcome = hatMetrics.SpaceOperationOutcomeHit
}
if err := orders.Record(
	hatMetrics.SpaceOperationRead,
	outcome,
	uint64(len(value)),
	time.Since(start),
); err != nil {
	return err
}

for _, snapshot := range registry.Snapshot() {
	fmt.Printf("%s: %d operations, %d bytes read\n",
		snapshot.Name, snapshot.Operations, snapshot.BytesRead)
}
```

For a hot path, retain the returned handle. Calling `orders.Record` avoids the
registry read lock and name lookup. `registry.Record("orders", ...)` is useful
when the name is only known at the call site and remains allocation-free, but it
does perform a map lookup for each event.

## Recorded Values

| Field | Meaning |
| --- | --- |
| `Operations` | Total accepted records. |
| `ReadOperations` | Records with kind `SpaceOperationRead`. |
| `WriteOperations` | Records with kind `SpaceOperationWrite`. |
| `DeleteOperations` | Records with kind `SpaceOperationDelete`. |
| `ScanOperations` | Records with kind `SpaceOperationScan`. |
| `Hits`, `Misses`, `Errors` | Outcome counters. |
| `BytesRead` | Caller-supplied bytes for read and scan records. |
| `BytesWritten` | Caller-supplied bytes for write and delete records. |
| `LatencyNanos` | Sum of caller-supplied non-negative latencies. |
| `LatencySamples` | Number of records included in the latency sum. |

Counters are monotonic `uint64` values and are safe for concurrent recording.
Snapshots are sorted by name and contain independent value copies. A snapshot
is a point-in-time report assembled from atomic loads; under concurrent writes,
adjacent fields can reflect slightly different instants.

The API intentionally records latency totals rather than allocating a
histogram or percentile structure for every space. Existing histogram or
tracing facilities should be used when percentile latency is required.

## Limits And Errors

- Registry names are trimmed, must be valid UTF-8, and are limited to 256 bytes.
- A registry accepts at most 65,536 names.
- Registering the same normalized name twice returns an error.
- Invalid operation kinds, outcomes, or negative latencies are rejected before
  any counter is changed.
- A registry is safe for concurrent registration, lookup, reporting, and
  convenience `Record` calls.

Applications should keep names bounded and intentional. Do not use an
unbounded request ID, user ID, or arbitrary URL as a metric name.

## Measurement

The benchmark runs five samples on the same machine and records the median:

```text
BenchmarkSpaceOperationMetricsRecord:       11.42 ns/op, 0 B/op, 0 allocs/op
BenchmarkSpaceOperationStatsRegistryRecord: 17.17 ns/op, 0 B/op, 0 allocs/op
```

The registry convenience path is about 1.50x the direct-handle time, or about
5.75 ns/op additional CPU time in this workload. Both paths allocate zero heap
bytes per event. The registry still consumes fixed handle and map memory per
registered name, so the limit is a protection against accidental label
cardinality growth, not a claim that the registry is free.

Run the reproducible benchmark with:

```text
make benchmark-t-u45
```

Correctness is covered by focused invalid-input, counter-total, snapshot
ownership, name-limit, and concurrent-recording tests in
`hat/hatMetrics/space_operation_stats_test.go`.
