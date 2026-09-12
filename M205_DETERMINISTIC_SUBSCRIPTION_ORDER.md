# Deterministic Differential Subscription Order

This is a Materialize-inspired reproducibility feature for differential query
subscriptions. Resolver, map, and execution-plan iteration can produce the
same logical result in different row orders. `DeterministicOrder` makes the
delta sequence stable for replay, snapshot comparison, and downstream
changefeed consumers.

## Usage

Set `DeterministicOrder` when creating a differential subscription:

```go
subscription, err := registry.SubscribeDifferential(ctx, hatSql.QuerySubscriptionDefinition{
    Query:              "FROM CACHE('people') SELECT id, name",
    Dependencies:       []string{"people"},
    DeterministicOrder: true,
}, resolver, hatSql.QueryOptions{})
```

The default is `false`. Existing subscriptions preserve their current order
and cost. The option applies only to `SubscribeDifferential`; normal snapshot
subscriptions are unchanged.

## Ordering Rules

- Initial and reset batches sort positive rows by the canonical binary row key.
- Update batches sort removals by canonical row key, followed by additions in
  canonical row-key order.
- Removal and addition phases remain separate so consumers that replace a row
  image continue to observe delete-before-insert behavior.
- Duplicate logical rows remain folded into one delta with their signed
  multiplicity.
- The logical result, frontier, revision, columns, and signed diffs do not
  change; only the order of equivalent batch entries changes.

## Measurement

`make benchmark-m205` uses 128 rows and five benchmark samples on the local
AMD Ryzen 9 5950X host:

| Mode | Median ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Resolver order | 81,211 | 74,974 | 1,031 |
| Deterministic order | 82,146 | 74,974 | 1,031 |

The opt-in sort measured approximately 1.01x the latency, with no material
allocation change. It is intentionally not enabled by default because this
feature improves reproducibility rather than throughput.

## Verification

```text
make format-m205
make test-m205
make benchmark-m205
```

The focused tests cover default resolver order, deterministic initial batches,
public subscription wiring, sorted update phases, multiplicity behavior, and
sorted reset batches.
