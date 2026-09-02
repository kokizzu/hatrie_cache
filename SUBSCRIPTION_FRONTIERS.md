# SQL Subscription Frontiers

`QuerySubscriptions` supports an opt-in frontier protocol inspired by
Materialize `SUBSCRIBE` progress. Existing `NotifyChanged` callers keep their
current behavior. Framed callers use `NotifyChangedAt` with a positive
sequence.

## Define A Subscription

```go
subscription, err := registry.Subscribe(ctx, hatSql.QuerySubscriptionDefinition{
    Query:        "FROM CACHE('people') SELECT name",
    Dependencies: []string{"people"},
    AsOf:         100,
    UpTo:         200,
    EmitProgress: true,
}, resolver, hatSql.QueryOptions{})
```

`AsOf` is the inclusive lower frontier. Events at or below it are ignored.
When `AsOf` is nonzero, `resolver` must implement
`HistoricalSourceResolver`; the initial query and framed updates resolve rows
at the requested frontier instead of silently reading the current state.

`UpTo` is optional. When a notification reaches or passes it, the subscription
is completed at exactly `UpTo`; later notifications are ignored. The update
channel closes after completion. `AsOf` must not be greater than `UpTo` when
both are set.

## Notifications

```go
err := registry.NotifyChangedAt(ctx, sequence, changed, resolver, options)
```

The call advances every live subscription to `sequence`. A dependency match
reevaluates the query; unrelated subscriptions only advance their frontier.
`EmitProgress` sends a `QuerySubscriptionSnapshot` with `Progress: true` and
an empty `Result`, which avoids copying the latest data result into a progress
record. Data updates have `Progress: false`. `Complete: true` marks the final
frontier. Updates retain the existing bounded, latest-value coalescing
behavior.

`NotifyChanged` remains the compatibility path for unframed changes. It does
not advance a numeric frontier or complete an `UpTo` subscription.

## Historical Resolver

The optional contract is deliberately small:

```go
type HistoricalSourceResolver interface {
    ResolveSQLSourceAt(name, key string, frontier uint64) ([]hatSql.Row, error)
}
```

The SQL executor uses the normal source resolver and therefore preserves all
existing query semantics. The historical wrapper intentionally falls back to
the general row path rather than pretending that a current-only index or
columnar snapshot is valid for an older frontier.

## Verification

```text
make test-sql-subscription-frontier
make benchmark-sql-subscription-frontier
```

On the recorded Linux/AMD Ryzen 9 5950X run, the five-sample median was:

| Path | Time (ns/op) | Heap (B/op) | Allocations |
| --- | ---: | ---: | ---: |
| Legacy unframed no-op | 162.9 | 160 | 1 |
| Framed frontier, no progress | 154.5 | 160 | 1 |
| Framed progress | 207.5 | 160 | 1 |

Progress is opt-in and adds about 1.28x CPU over the legacy no-op in this
fixture, while the progress payload itself stays allocation-bounded and does
not clone the result rows.
