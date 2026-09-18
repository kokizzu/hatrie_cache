# MZ-010 SQL Result Subscription Entrypoints

Status: partially adopted.

Materialize-style query-result subscriptions are available through the
importable `hat/hatSql` package. The entrypoints reuse the existing bounded
snapshot and differential subscription queues; they do not start a server or
background goroutine.

## API

```go
func SQLQueryDependencies(source string, parameters []interface{}) ([]string, error)

func (registry *QuerySubscriptions) SubscribeSQL(
    ctx context.Context,
    definition QuerySubscriptionDefinition,
    resolver SourceResolver,
    options QueryOptions,
) (*QuerySubscription, error)

func (registry *QuerySubscriptions) SubscribeDifferentialSQL(
    ctx context.Context,
    definition QuerySubscriptionDefinition,
    resolver SourceResolver,
    options QueryOptions,
) (*QueryDifferentialSubscription, error)
```

When `definition.Dependencies` is empty, the SQL entrypoints parse the query,
collect static `CACHE(...)` sources from the main query, joins, CTEs, and set
operation branches, and sort them deterministically. Positional parameters are
bound before dependency extraction, so `FROM CACHE($1)` is supported when the
parameter resolves to a source name.

All other definition fields remain available, including `Parameters`,
`AsOf`, `UpTo`, `EmitProgress`, `StartLive`, and `DeterministicOrder`.
Callers with dynamic or non-`CACHE` sources can provide `Dependencies`
explicitly; that bypasses automatic discovery and preserves the existing
subscription behavior.

## Example

```go
definition := hatSql.QuerySubscriptionDefinition{
    Query:        "FROM CACHE('people') SELECT id, name",
    EmitProgress: true,
    StartLive:    true,
}
subscription, err := registry.SubscribeSQL(ctx, definition, resolver, hatSql.QueryOptions{})
if err != nil {
    return err
}
defer subscription.Close()

for update := range subscription.Updates() {
    consume(update.Frontier, update.Result.Rows, update.Progress)
}
```

Use `SubscribeDifferentialSQL` when consumers need signed row multiplicities
instead of complete result snapshots. A positive `Diff` inserts a row and a
negative `Diff` retracts it. Existing bounded coalescing semantics still apply;
when a differential queue coalesces, `Reset` tells the consumer to rebuild
from the positive batch.

## Cost And Limits

Automatic dependency discovery is opt-in and happens only when a subscription
is created. It adds one parser/dependency pass at registration; refresh and
delivery use the same code path as explicit `Subscribe` calls. On the local
AMD Ryzen 9 5950X benchmark, explicit dependencies had a median of about
`6.66 us`, `5.7 KB`, and 28 allocations per registration, while automatic
discovery had `8.78 us`, `8.1 KB`, and 32 allocations. Use explicit dependencies
for hot churn of short-lived subscriptions or dynamic source graphs.

This does not add `TAIL` or `SUBSCRIBE` SQL statement grammar, cross-process
transport, or a signed network envelope. Those remain separate features so
the current query and transport contracts are not changed implicitly.
