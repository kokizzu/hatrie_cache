# T-U53 Logical-Time Frontier

## Status

Adopted as the opt-in `hatDataStructure.LogicalFrontier` primitive. It tracks
monotonic scalar logical timestamps for bounded named sources, exposes the
minimum source timestamp, provides deterministic isolated snapshots, and lets
callers wait for a target frontier with context cancellation.

No existing write, replication, SQL, or changefeed path is changed. Callers
must explicitly create and advance a frontier.

## Inspiration and Boundary

Materialize and timely dataflow use progress frontiers to decide when an
event-time result is complete and when state can be compacted. ClickHouse-style
event-time processing benefits from the same distinction between observed
data and a safe-to-finalize watermark. Tarantool's explicit replication and
queue progress also support bounded coordination when consumers advance at
different rates.

This implementation intentionally supports one ordered `uint64` timestamp per
source. It is not a multidimensional antichain, does not infer time from wall
clock values, and does not perform compaction or retention by itself. Those
policies remain caller-owned.

## Example

```go
frontier, err := hatDataStructure.NewLogicalFrontier(
	hatDataStructure.DefaultLogicalFrontierConfig(),
)
if err != nil {
	return err
}
if err := frontier.Register("eu", 100); err != nil {
	return err
}
if err := frontier.Register("us", 95); err != nil {
	return err
}

if err := frontier.Wait(ctx, 100); err != nil {
	return err
}
// The wait completes after both sources reach logical time 100, or a source
// is explicitly unregistered.
```

`Advance` rejects regressions, treats equal timestamps as a no-op, and returns
whether progress changed. `Unregister` explicitly removes a source; this is
important for a failed or retired input and is never inferred from silence.

An empty frontier has no current timestamp and is considered complete by
`Wait`, because there are no registered unfinished inputs.

## Bounds

| Resource | Default | Hard cap |
| --- | ---: | ---: |
| Sources | 1,024 | 65,536 |
| Source-name bytes | 256 | 4,096 |

The source map is allocated lazily. Frontier advancement only rescans all
sources when the source that held the cached minimum moves; advancing a source
already above the minimum uses the cached value. Waiters share a notification
channel and do not create one goroutine each.

## Verification and Measurement

Focused tests cover monotonicity, regression rejection, cancellation,
frontier waiting, source limits, deterministic snapshot ordering, snapshot
isolation, and source removal. The package passes race and vet checks.

Five-sample median on AMD Ryzen 9 5950X, Linux amd64:

| Operation | Baseline | Frontier | Frontier memory |
| --- | ---: | ---: | ---: |
| Minimum over 64 values / non-minimum source advance | 33.45 ns/op, 0 allocs | 26.33 ns/op, 0 allocs | 0 B/op |
| 64-source deterministic snapshot | not applicable | 6,994 ns/op, 4 allocs | 1,888 B/op |

The first row is a control-plane comparison: the baseline rescans all 64
values while the frontier updates one named source and retains the minimum.
The feature is a win for repeated progress checks, while snapshots have an
intentional copy-and-sort cost and should be sampled rather than taken on every
row update.
