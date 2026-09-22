# M230: Source Backpressure From the Downstream Frontier

## What changed

`SpaceChangefeed` can now use an opt-in downstream frontier to bound how far
the source may advance beyond its slowest active subscriber. The feature is
disabled by default, so existing feeds retain their nonblocking publish and
subscriber-overflow behavior.

```go
feed, err := hatReplication.NewSpaceChangefeed(hatReplication.SpaceChangefeedOptions{
	Space:         "orders",
	SchemaVersion: "v1",
	Backpressure: hatReplication.SpaceChangefeedBackpressureOptions{
		Enabled: true,
		MaxLag:  1024,
	},
})
```

`MaxLag` is measured in feed sequence numbers. Before assigning the next
sequence, the feed compares it with the minimum acknowledged checkpoint among
active subscribers. When the distance is too large, `Publish` returns
`ErrSpaceChangefeedBackpressure` without advancing the sequence, retaining the
event, or delivering a partial result. The caller can retry after consumers
call `Advance`, or apply its own write-rate policy.

There is no waiting goroutine or condition variable. A feed with no active
subscribers is never backpressured. Closed, overflowed, and cancelled
subscribers are removed from the frontier. `MaxLag` may be zero when the
caller wants every publish to wait for an acknowledgement.

## Observability

`SpaceChangefeedStats` reports:

- `BackpressureEnabled`: whether the option is enabled.
- `DownstreamFrontier`: the minimum acknowledged sequence, or zero with no subscribers.
- `MaxDownstreamLag`: the current distance from `NextSequence` to that frontier.
- `BackpressuredPublishes`: the number of rejected publish attempts.

The minimum frontier is cached. Normal publication does not scan the
subscriber map; the cache is updated when a subscriber is added, advances, or
closes. This keeps the opt-in check close to constant time while preserving
the slowest-subscriber semantics.

## Tradeoff measurement

Machine: AMD Ryzen 9 5950X, Linux amd64. Command:
`make benchmark-m230-backpressure` (`go test ... -benchmem -count=5`).
Values below are the five raw samples from the run; median is used for the
comparison.

| Workload | Before frontier cache | After frontier cache | Change | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Legacy publish with acknowledged subscriber | 210.3, 220.5, 228.6, 251.8, 240.8 ns/op (median 228.6) | 297.7, 282.9, 289.5, 276.1, 301.4 ns/op (median 289.5) | separate run; workload baseline variance | 16 B/op, 2 allocs/op |
| Enabled publish with acknowledged subscriber | 289.9, 304.7, 289.9, 277.3, 290.7 ns/op (median 289.9) | 289.6, 314.0, 311.8, 293.5, 282.2 ns/op (median 293.5) | cached run is about 1.4% above its paired baseline | 16 B/op, 2 allocs/op |
| Rejected publish | 122.7, 117.6, 123.4, 103.7, 102.9 ns/op (median 117.6) | 83.30, 77.68, 82.61, 80.87, 66.61 ns/op (median 80.87) | about 31% lower in the later run | 8 B/op, 1 alloc/op |

The runs are separate five-sample executions on a shared development host, so
the absolute legacy medians are not a controlled A/B result. The useful
signal is that caching removed the earlier roughly 27% enabled-path overhead
from scanning subscribers on every publish; the final enabled path was within
normal run-to-run noise of the legacy path and did not increase allocations.

## Verification

- `make test-m230-backpressure`: focused behavior tests.
- `make test-m230-backpressure-package`: complete `hat/hatReplication` package tests.
- `make race-m230-backpressure`: focused race test.
- `make vet-m230-backpressure`: package vet.

The tests cover default-off behavior, lag rejection and retry, the slowest of
multiple subscribers, configuration bounds, frontier/lag statistics, and
removing a disconnected slow subscriber.
