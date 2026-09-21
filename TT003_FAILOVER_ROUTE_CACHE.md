# TT-003 Failover Route Cache

`hatReplication.FailoverRouteCache` is an opt-in client/router control-plane
cache for a bounded set of topology routes. It provides stable key-based
selection, strict topology-generation replacement, and health-aware failure
suppression without changing existing replication or connection-pool defaults.

## Usage

```go
cache, err := hatReplication.NewFailoverRouteCache(hatReplication.FailoverRouteCacheOptions{
	MaxRoutes:        256,
	FailureThreshold: 2,
	Cooldown:         5 * time.Second,
})
if err != nil {
	return err
}

err = cache.Replace(42, []hatReplication.FailoverRoute{
	{NodeID: "node-a", Address: "a.internal:9000"},
	{NodeID: "node-b", Address: "b.internal:9000"},
})
route, err := cache.Lookup("tenant-17", time.Now())
```

Topology updates must use a strictly newer generation. A route failure is
reported by node identity, not by address, so a later topology replacement can
change an endpoint without inheriting stale health state:

```go
if err := cache.ReportFailure(route.NodeID, time.Now()); err != nil {
	return err
}
// A later lookup skips the route during its configured cooldown.
```

`ReportSuccess` clears suppression immediately. `Invalidate` forces a
cooldown, and `Snapshot` exposes bounded health state for monitoring.

## Cost and boundaries

Lookups load an immutable atomic snapshot, hash the key, and scan at most the
configured route count. They take no mutex and allocate nothing. Replacements
and health reports copy the bounded route slice under a short control-plane
mutex. The cache does not dial, retry, perform consensus, or infer health from
transport errors; callers own those policies and explicitly report outcomes.

The defaults are 1,024 routes, one failure before suppression, and a five-second
cooldown. Route IDs and addresses are size-bounded and duplicate node IDs are
rejected. The feature is opt-in, so applications that already select routes
directly retain their existing behavior and cost.

## Benchmark

Five runs on Linux amd64, AMD Ryzen 9 5950X, `-cpu=1`, using 16 healthy routes
and four repeated tenant keys:

| Path | Median ns/op | B/op | allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Existing caller-side scan | 7.868 | 0 | 0 | 1.00x |
| Atomic route-cache lookup | 12.35 | 0 | 0 | 1.57x slower |

The cache intentionally costs about 4.5 ns/op over a private static slice in
this microbenchmark. That cost buys concurrent generation replacement and
health suppression; it is not enabled by default. Route replacement and
health-report copy costs are control-plane operations and are excluded from
the lookup benchmark.

Reproduce with:

```text
make benchmark-tt003-route-cache-baseline
make benchmark-tt003-route-cache
```
