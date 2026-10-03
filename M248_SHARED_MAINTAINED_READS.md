# M248: Reusable Maintained Read Sharing

`QuerySubscriptions` can optionally share one successful evaluation between
multiple subscriptions that ask for the same read expression during one
refresh notification.

## Usage

Set `ShareIdenticalReads` on every subscription that is safe to coalesce:

```go
definition := hatSql.QuerySubscriptionDefinition{
	Query:               "FROM CACHE('people') SELECT id ORDER BY id",
	Dependencies:        []string{"people"},
	ShareIdenticalReads: true,
}
subscription, err := registry.Subscribe(ctx, definition, resolver, options)
```

Sharing is default-off. Existing callers and subscriptions without the flag
retain the original execution behavior.

The memoization scope is one `NotifyChanged` or `NotifyChangedAt` call. A
result is reusable only when the query text, parameters, and historical source
frontier match. The result is cloned before each subscription publishes it, so
subscribers do not share mutable row storage. A failed evaluation is never
cached, and no result is retained after the refresh call returns.

This option is intended for deterministic, read-only resolvers. A resolver
with intentional per-execution side effects should leave it disabled.

## Measurement

The benchmark used 64 identical subscriptions, a 1,024-row source, one source
row changed per refresh, five samples per case, and `-benchmem` on the same
AMD Ryzen 9 5950X host. The default path was measured against the detached base
worktree; the shared case is the opt-in feature path.

| Case | Time/op | Bytes/op | Allocs/op | Source resolves/op | Relative time |
| --- | ---: | ---: | ---: | ---: | ---: |
| Base, sharing unavailable | 56.13 ms | 112,538,406 | 653,959 | 64 | 1.00x |
| Feature, default-off | 56.68 ms | 112,538,406 | 653,959 | 64 | 0.99x |
| Feature, opt-in sharing | 33.33 ms | 68,104,289 | 396,426 | 1 | 1.68x |

The opt-in path is about 1.68x faster, uses about 39% fewer bytes, performs
about 39% fewer allocations, and removes 63 of 64 duplicate source reads. The
default path remains within roughly 1% of the base measurement and does not
retain a sharing map.
