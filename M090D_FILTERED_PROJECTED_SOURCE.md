# M090d Filtered Projected Sources

M090d extends the independent compute/storage boundary with an optional
predicate-aware projected-source contract. A stateless compute process can ask
remote storage for only the selected fields and rows that can satisfy the
planner's literal `WHERE` predicates.

```go
type RemoteSource struct{}

func (RemoteSource) ResolveSQLProjectedSourceContextWithPredicates(
	ctx context.Context,
	name, key string,
	fields []string,
	predicates []hatSql.SQLPartitionPredicate,
) ([]hatSql.Row, bool, error) {
	// Fetch only fields and rows matching every predicate.
	return fetchProjected(ctx, name, key, fields, predicates)
}
```

The non-context variant is
`ResolveSQLProjectedSourceWithPredicates`. The context-aware variant wins when
both are implemented. Returning `available=false` falls back to the existing
projected or full-row resolver. Existing resolvers do not change behavior.

Safety contract:

- The predicate list contains only planner-proven literal predicates joined by
  `AND`, with binary collation. It can include `=`, `IN`, `<`, `<=`, `>`, `>=`,
  and `VALID_AT`.
- Storage may omit only rows that cannot satisfy every supplied predicate.
  Omitting a possible match is incorrect.
- The SQL executor still evaluates the original `WHERE` expression after the
  fetch, so false positives from a conservative storage filter are harmless.
- A resolver that implements partition pruning keeps the existing partition
  path; this interface is for row-level pruning at a remote source boundary.
- The feature is opt-in. No resolver is required to implement it, and it does
  not create a network connection or change the default storage path.

## Measurement

Command:

```text
make benchmark-m090d-filtered-projected-source
```

The benchmark runs a deterministic 4,096-row source where half the rows match
`region = 'eu'`. It compares the legacy full-row resolver with a resolver that
filters rows and transfers only `id` and `region`. `source-bytes/op` is a
deterministic payload-size proxy, not a wire capture; allocations and bytes
include the SQL executor and resolver callback.

Raw samples from `linux/amd64`, AMD Ryzen 9 5950X, Go benchmark `-cpu 32`:

| Path | ns/op | source-bytes/op | B/op | allocs/op |
| --- | ---: | ---: | ---: | ---: |
| legacy full row | 3,246,697; 3,194,120; 3,211,002; 3,214,116; 3,340,707 | 598,016 | 4,104,130; 4,104,110; 4,104,111; 4,104,115; 4,104,111 | 20,512 |
| filtered projected | 1,686,168; 1,700,375; 1,704,311; 1,704,099; 1,717,246 | 20,480 | 2,484,476; 2,484,550; 2,484,552; 2,484,549; 2,484,547 | 12,327 |

Median comparison:

- `1.89x` lower query time (`3,214,116` to `1,704,099` ns/op)
- `29.20x` lower projected source payload proxy (`598,016` to `20,480`)
- `1.65x` lower allocated bytes/op
- `1.66x` fewer allocations/op

The tradeoff is resolver complexity and a correctness obligation for remote
predicate evaluation. Because the contract is opt-in and `available=false`
preserves the old path, services can enable it only for storage engines with a
trusted predicate implementation.
