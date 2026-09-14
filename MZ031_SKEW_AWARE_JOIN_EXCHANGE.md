# MZ-031 Skew-Aware Join Exchange

`hatSql.SkewAwareJoinExchange` is an imported placement primitive inspired by
Materialize’s skew-aware dataflow exchange. It detects heavy join keys and
returns deterministic routing decisions without changing the SQL planner or
claiming to execute a distributed join by itself.

## Routing Model

- Cold keys are routed to one worker using a stable FNV-1a hash of the join
  key. Both join sides therefore share one owner.
- A key becomes hot after its observed weight reaches
  `HotKeyThreshold`.
- Hot rows on the configured `BroadcastSide` are routed to every worker so
  each local join arrangement has the build-side state.
- Hot rows on the other side are routed by source-row key, spreading probes
  across workers while preserving one placement per source row.
- Promotion increments a generation and sets `RebalanceRequired`. A caller
  must rehydrate the promoted key at that generation and call
  `AcknowledgeRebalance`; stale acknowledgements cannot clear a newer plan.
- Frequency tracking is bounded by `MaxTrackedKeys`; cold keys beyond that
  limit are ignored unless one observation already reaches the threshold.

The caller must deduplicate or otherwise coordinate output according to its
join protocol. Broadcasting build rows is state replication, not permission
to emit duplicate joined results. The exchange does not move existing state;
the generation fence makes that required rehydration explicit.

## Example

```go
exchange, err := hatSql.NewSkewAwareJoinExchange(
	hatSql.SkewAwareJoinExchangeOptions{
		Workers:        8,
		HotKeyThreshold: 1024,
		BroadcastSide:  hatSql.IncrementalJoinRight,
	},
)
if err != nil {
	return err
}

if _, err := exchange.Observe("tenant:hot", 1024); err != nil {
	return err
}
route, err := exchange.Route(
	hatSql.IncrementalJoinLeft,
	"tenant:hot",
	"event:123",
)
if err != nil {
	return err
}
if route.RebalanceRequired {
	// Rehydrate the key at route.Generation, then:
	_ = exchange.AcknowledgeRebalance("tenant:hot", route.Generation)
}
if route.Broadcast {
	for _, worker := range route.Workers(8) {
		_ = worker
	}
} else {
	_ = route.Worker
}
```

Zero-valued optional settings use a threshold of 1,024, a right-side build
broadcast, and a 65,536-key cold tracking bound. `Workers` is required; a
single-worker exchange is valid but provides no skew relief.

## Measured Result

The benchmark processes the same 100,000 left-side events on four workers:
80,000 share one hot join key and 20,000 use 1,000 cold keys.

| Policy | Median per 100k events | Allocations | Maximum worker load | Result |
| --- | ---: | ---: | ---: | --- |
| Naive join-key hash | 351,651 ns | 0 | 85,000 | 1.00x routing CPU |
| Skew-aware exchange | 4,019,681 ns | 0 | 25,000 | 11.4x routing CPU, 3.4x lower max load |

The routing primitive itself is slower because it validates keys, checks the
hot-key policy, and hashes hot probe source keys. The measured value is the
3.4x lower serialized worker load; end-to-end throughput improves only when
join processing is the bottleneck and its cost scales with worker load. The
policy is therefore opt-in and should be benchmarked with the real join
executor before enabling it.

The atomic hot-routing snapshot optimization changed the skew-aware median
from 4,161,421 ns to 4,019,681 ns per 100,000 events, about 1.035x faster,
without changing load distribution or allocations.

Raw five-sample output after the optimization:

```text
BenchmarkMZ031NaiveJoinExchange-32
355623 ns/op  85000 max-load  0 B/op  0 allocs/op
346113 ns/op  85000 max-load  0 B/op  0 allocs/op
349141 ns/op  85000 max-load  0 B/op  0 allocs/op
356571 ns/op  85000 max-load  0 B/op  0 allocs/op
351651 ns/op  85000 max-load  0 B/op  0 allocs/op

BenchmarkMZ031SkewAwareJoinExchange-32
4019681 ns/op  25000 max-load  0 B/op  0 allocs/op
3878043 ns/op  25000 max-load  0 B/op  0 allocs/op
3709334 ns/op  25000 max-load  0 B/op  0 allocs/op
4043679 ns/op  25000 max-load  0 B/op  0 allocs/op
4127233 ns/op  25000 max-load  0 B/op  0 allocs/op
```

## Verification

Tests cover promotion thresholds, deterministic cold routing, build-side
broadcast, source-key probe spreading, generation fencing, stale
acknowledgements, bounded tracking, validation, and exact skewed-load counts.

```text
make verify-mz031-skew-aware-join-exchange
make benchmark-mz031-skew-aware-join-exchange
```
