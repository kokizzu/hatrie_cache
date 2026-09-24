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

## Statement Grammar

`ParseSQLSubscriptionStatement` adds an importable, opt-in statement envelope
without changing the existing query grammar:

```sql
SUBSCRIBE FROM CACHE('people') SELECT id, name
SUBSCRIBE SNAPSHOT FROM CACHE('people') SELECT id, name
SUBSCRIBE DIFFERENTIAL FROM CACHE('people') SELECT id, name
TAIL FROM CACHE('people') SELECT id, name
```

`SUBSCRIBE` defaults to snapshot mode. `TAIL` is an alias for differential
mode. The returned `SQLSubscriptionStatement.Definition` can be passed to
`SubscribeSQL` or `SubscribeDifferentialSQL` according to its `Mode`; those
existing methods still perform query validation, dependency discovery, and
source resolution.

## Signed wire envelopes

An external transport can opt into a bounded binary envelope when subscription
payloads need integrity and origin authentication:

```go
wire, err := hatSql.SealSQLSubscriptionWireEnvelope(key, hatSql.SQLSubscriptionWireEnvelope{
    Mode:           hatSql.SQLSubscriptionModeDifferential,
    SubscriptionID: "people-live",
    Sequence:       42,
    Diff:           1,
    Payload:        payload,
})
envelope, err := hatSql.OpenSQLSubscriptionWireEnvelope(key, wire)
```

The envelope uses a fixed header and HMAC-SHA256, verifies with a
constant-time comparison, rejects empty or oversized keys, limits the
subscription ID to 256 bytes and the payload to 16 MiB, and copies decoded
payloads so they do not alias the input buffer. Snapshot envelopes require a
zero differential weight. This API does not implicitly alter existing
transports or provide key rotation; callers should manage key distribution and
rotation outside the envelope.

The measured five-sample medians on the repository benchmark are approximately
52.7 ns/op and 96 B/op for unsigned framing, 703.8 ns/op and 640 B/op for
sealing, and 744.9 ns/op and 592 B/op for opening. The authenticated frame adds
32 bytes for the MAC and uses 7 allocations to seal or 8 to open versus one
allocation for the unsigned baseline, so it is appropriate for integrity
boundaries rather than a default hot path.

## Cost And Limits

Automatic dependency discovery is opt-in and happens only when a subscription
is created. It adds one parser/dependency pass at registration; refresh and
delivery use the same code path as explicit `Subscribe` calls. On the local
AMD Ryzen 9 5950X benchmark, explicit dependencies had a median of about
`6.66 us`, `5.7 KB`, and 28 allocations per registration, while automatic
discovery had `8.78 us`, `8.1 KB`, and 32 allocations. Use explicit dependencies
for hot churn of short-lived subscriptions or dynamic source graphs.

This does not add a cross-process transport, key rotation, or automatic
integration with the current transport contracts; those remain caller-owned.
