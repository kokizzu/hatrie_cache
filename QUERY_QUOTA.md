# Query Quotas

`NamespaceResourceLimits.MaxQueriesPerWindow` and `QueryWindow` add a
fixed-window request quota to a namespace. A zero maximum disables the quota;
a positive maximum with a zero window uses one minute. Quota state is created
lazily per namespace, and `ErrNamespaceQueryRateLimited` is returned before
SQL execution once the window allowance is exhausted.

Namespace-specific values only tighten the defaults, so one governor can
represent user, tenant, or source policies. The quota is independent from
`MaxConcurrentQueries` and `MaxQueuedQueries`.

The disabled quota lookup remains allocation-free in the focused benchmark at
about `3.4-3.6 ns/op`, `0 B/op`, and `0 allocs/op` on the benchmark host.
Verify with `make test-query-quota`, `make race-query-quota`, and
`make benchmark-query-quota`.
