# M-U39 Partition And Order Declarations

`SQLSourceLayoutResolver` is an optional source-resolver contract for exposing
bounded physical layout metadata to SQL planning diagnostics. It is useful for
systems that already partition or sort data outside the SQL executor and want
that knowledge to be visible to `EXPLAIN`.

```go
type SQLSourceLayoutResolver interface {
	ResolveSQLSourceLayout(name, key string) (SQLSourceLayout, bool, error)
}

type SQLSourceLayout struct {
	PartitionBy []string
	OrderBy     []SQLSourceOrder
}

type SQLSourceOrder struct {
	Field      string
	Descending bool
	NullsFirst bool
	NullsLast  bool
}
```

The resolver receives the same source name and key as `ResolveSQLSource`. A
successful declaration is copied before it is retained in an explanation, so
the resolver can safely reuse or mutate its own backing slices after the call.

## Behavior

- The contract is optional. Existing resolvers and queries are unchanged when
  they do not implement it.
- Declarations appear in the existing `EXPLAIN` `arrangements` envelope as an
  entry with `kind: "partition_order"` and a nested `layout` object.
- The same metadata is available through `EXPLAIN PIPELINE` because that path
  reuses the source explanation step.
- Resolver errors, unavailable metadata, malformed fields, and excessive
  declarations are ignored for diagnostics; they do not make an otherwise
  valid query fail.
- A source-local temporary table keeps its existing precedence over the
  underlying resolver.
- The declaration does not rewrite data, create an index, or automatically
  prune partitions. It is a bounded planning signal that callers can use when
  adding their own partition pruning or ordered-read integration.

The safety bounds are 16 partition/order fields and 256 bytes per field.
Duplicate fields within either declaration list are rejected
case-insensitively, and an order declaration cannot request both `NULLS FIRST`
and `NULLS LAST`.

Example resolver output:

```json
{
  "key": "source_layout",
  "kind": "partition_order",
  "recommendation": "declared source partition/order",
  "layout": {
    "partition_by": ["region"],
    "order_by": [
      {"field": "created_at", "descending": true, "nulls_last": true}
    ]
  }
}
```

## Measurements

The benchmark measures the regular `EXPLAIN` path with five samples per case.
The default path is the important comparison: adding the optional contract did
not change the `ExplainStep` size or default allocations.

| Case | Samples (ns/op) | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Before, resolver without layout | 7620, 7951, 7935, 8115, 8229 | 7951 | 9760 | 51 |
| After, resolver without layout | 8362, 8058, 8028, 8194, 8499 | 8194 | 9760 | 51 |
| After, resolver with layout | 9445, 9496, 9347, 9545, 9569 | 9496 | 10440 | 69 |

The opt-in layout explanation adds 680 bytes and 18 allocations to that
diagnostic response. It is not a claim of faster query execution; the feature
keeps the default execution and explanation allocation profile unchanged.

The focused correctness and benchmark commands are:

```text
make codex-mu39-test
make codex-mu39-benchmark
```
