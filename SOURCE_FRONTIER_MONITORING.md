# Source Frontier Monitoring

Source-frontier metrics are optional. Set `MonitoringOptions.SourceFrontier`
to a `hatMetrics.SourceFrontierRegistry`, and set
`MonitoringOptions.SourceFrontierObserved` to a function returning the current
global observed frontier.

With both options configured, `/metrics` exposes deterministic gauges:

- `hatrie_cache_source_frontier{node,source}`: latest frontier recorded for a source.
- `hatrie_cache_source_observed{node}`: global observed frontier returned by the callback.
- `hatrie_cache_source_lag{node,source}`: observed frontier minus source frontier, clamped at zero.

The registry rejects regressed source frontiers. Source names are trimmed and
metrics are emitted in source-name order. Prometheus label escaping is applied
before output. If `SourceFrontier` is nil, no source-frontier metrics are
emitted. If the observed callback is nil, only the source frontier gauge is
emitted; lag is never reported as an invented zero.
