# Source Frontier Metrics

`hatMetrics.SourceFrontierRegistry` records progress for independent source
partitions or feeds. It is a small Materialize-style observability primitive:
callers advance a named source frontier as input is processed, then take a
snapshot against the frontier currently observed by a consumer.

```go
registry := hatMetrics.NewSourceFrontierRegistry()
_ = registry.Advance("orders-us", 120)
_ = registry.Advance("orders-eu", 117)

rows := registry.Snapshot(120)
// orders-eu: Frontier=117, Observed=120, Lag=3
// orders-us: Frontier=120, Observed=120, Lag=0
```

Updates are monotone per source. A regressed update returns
`hatMetrics.ErrSourceFrontierRegressed` and leaves the previous value intact.
Source names are trimmed and must be non-empty. Snapshots are sorted by source
name, own their returned memory, and clamp lag to zero when the observed
frontier is older than a source.

The registry is in-memory metrics state. It does not change replication or
durability semantics; persist or export the snapshots through the caller's
existing monitoring path when needed.
