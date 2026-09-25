# CH-035 Remote Shard Pruning

Status: implemented as an opt-in `hatSql` resolver contract.

This adopts the ClickHouse idea of routing a query only to remote shards that
can contain matching rows. Existing `ExecuteSQLDistributedQuery` callers keep
their behavior unless a shard resolver implements
`SQLDistributedQueryShardPruner`.

## API

```go
type SQLDistributedQueryShardPruner interface {
	ShouldQuerySQLDistributedShard(
		name string,
		key string,
		predicates []SQLPartitionPredicate,
	) (include bool, available bool, err error)
}
```

The coordinator parses and binds the query once when at least one shard
supports the interface. It passes the source kind, source key, and the same
safe literal predicates already used by local partition pruning. A resolver
returns:

- `include=true`: query this shard;
- `include=false`: skip this shard, because it proves no row can match;
- `available=false`: retain the shard and use the normal distributed path;
- `err != nil`: abort before remote reads and wrap the error with
  `ErrSQLDistributedQueryShardPruning`.

The adapter must be conservative. It must never skip a shard that could contain
a matching row. The original `WHERE` expression is still evaluated on every
selected shard.

## Safe Predicate Scope

Automatic extraction is intentionally narrow and fail-safe:

- direct `CACHE` or `KEYS` sources;
- literal or bound-parameter `=`, `IN`, `<`, `<=`, `>`, and `>=` predicates;
- predicates joined by `AND`;
- binary collation and no `TABLESAMPLE`, join, or dynamic expression.

`OR`, unsupported operators, unavailable metadata, and unsupported query shapes
retain the normal fan-out. Existing resolvers need no changes.

If every shard is safely excluded, the default merger evaluates the query once
against an empty local source. This preserves projected columns and aggregate
shape, including `COUNT(*) = 0`. A custom merge receives an empty shard-result
list and remains responsible for its empty-input semantics.

## Example Adapter

An adapter can route a region-partitioned cluster without parsing SQL itself:

```go
func (r regionResolver) ShouldQuerySQLDistributedShard(
	_ string,
	_ string,
	predicates []hatSql.SQLPartitionPredicate,
) (bool, bool, error) {
	if len(predicates) != 1 {
		return true, false, nil
	}
	predicate := predicates[0]
	if predicate.Field != "region" || predicate.Operator != "=" || len(predicate.Values) != 1 {
		return true, false, nil
	}
	return predicate.Values[0] == r.region, true, nil
}
```

The adapter must return `available=false` when its metadata is stale or cannot
prove exclusion. It should not guess from shard names.

## Measurement

The benchmark uses eight shards, a 1 ms remote read delay, bounded
`MaxConcurrency=2`, `GOMAXPROCS=8`, five samples, and `-benchmem` on Linux/amd64
with an AMD Ryzen 9 5950X. The baseline executes all eight shards; CH-035
retains one and skips seven.

| Workload | Median ns/op | Median B/op | Median allocs/op | Result |
| --- | ---: | ---: | ---: | --- |
| All eight shards | 4,574,994 | 48,981 | 197 | control |
| Seven shards pruned | 1,096,408 | 10,894 | 39 | 4.17x faster, 4.50x lower bytes, 5.05x fewer allocations |

Raw samples:

```text
all_shards:
4610562 ns/op 49076 B/op 197 allocs/op
4619364 ns/op 48990 B/op 197 allocs/op
4444625 ns/op 48981 B/op 197 allocs/op
4573996 ns/op 48974 B/op 197 allocs/op
4574994 ns/op 48978 B/op 197 allocs/op

pruned_7_of_8:
1121203 ns/op 10895 B/op 39 allocs/op
1093263 ns/op 10894 B/op 39 allocs/op
1096408 ns/op 10894 B/op 39 allocs/op
1098944 ns/op 10893 B/op 39 allocs/op
1095228 ns/op 10894 B/op 39 allocs/op
```

The existing no-pruner distributed path was rerun before and after the change
with its original eight-worker benchmark. Its median moved from 1,283,275 to
1,274,169 ns/op, from 37,082 to 36,842 B/op, and stayed at 152 allocs/op.
That is within benchmark noise and shows no default-path regression.

The improvement depends on remote work and selectivity. For cheap local
resolvers, the extra metadata callbacks and one coordinator parse may not pay
back; adapters should return unavailable when they cannot prove exclusion.
