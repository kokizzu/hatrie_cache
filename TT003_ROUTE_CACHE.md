# TT-003 Health-Aware Peer Route Cache

This feature adds an importable, bounded route cache for callers that have
multiple addresses for one logical peer. It is inspired by Tarantool-style
router failover, but it does not open connections or make topology decisions
on behalf of the caller.

## API

```go
cache, err := hatReplication.NewPeerRouteCache(hatReplication.PeerRouteCacheOptions{})
if err != nil {
	return err
}
if err := cache.Replace("orders", []hatReplication.PeerRoute{
	{Node: "orders-a", Address: "10.0.0.11:3301"},
	{Node: "orders-b", Address: "10.0.0.12:3301"},
}); err != nil {
	return err
}

route, err := cache.Do(ctx, "orders", func(ctx context.Context, route hatReplication.PeerRoute) error {
	return dialAndSend(ctx, route.Address)
})
_ = route
return err
```

`Do` tries each eligible route at most once. A non-context error cools the
failed route for `FailureCooldown`; a successful route loses its failure
penalty. Context cancellation and deadlines are returned immediately and do
not mark a route unhealthy. `Replace` atomically replaces one logical target,
copies the route slice, validates unique non-empty node names and addresses,
and resets old health state. `Invalidate` removes both routes and health state.

The zero-value options are bounded defaults: at most 32 routes per key, all
configured routes are eligible for one `Do` call, and failed routes cool down
for two seconds. The cache is concurrency-safe. It is disabled unless an
application constructs and uses it, so existing peer and replication paths
are unchanged.

## Operational Boundaries

- Refresh routes from the authoritative membership/topology source after a
  membership change; call `Invalidate` when a target is removed.
- Keep `Node` stable when an address changes so health follows the logical
  peer only through an explicit `Replace`, which resets stale health.
- Do not put credentials in `PeerRoute.Address`; authentication remains the
  responsibility of the transport setup.
- The cache is process-local. It does not provide ownership consensus,
  replication, or a cross-process health view.

## Measurement

Five `-count=5` samples on Linux/amd64, AMD Ryzen 9 5950X, with 32 routes:

| Workload | Median ns/op | B/op | Allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| Raw round-robin slice selection | 0.8675 | 0 | 0 | lower-bound CPU control |
| `PeerRouteCache.Next` | 188.7 | 0 | 0 | health/cooldown/rotation state |
| `PeerRouteCache.Do` after one cooled failure | 240.8 | 0 | 0 | one transport attempt, no retry of the failed route |

The route-cache selection path is intentionally slower than a raw slice pick;
this is the cost of synchronized health state and bounded failover. The
measured benefit is behavioral: the failover fixture performs one eligible
transport attempt instead of the raw sequential control's two attempts. The
cache adds no heap allocation to these hot paths and retains only bounded
per-route state.
