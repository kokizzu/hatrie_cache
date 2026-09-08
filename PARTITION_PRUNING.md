# SQL Partition Pruning

`hatSql` provides an optional `PartitionPruningSourceResolver` extension for
partitioned `CACHE(...)` and `KEYS(...)` sources. The executor extracts only
planner-proven literal predicates and asks the resolver for a complete subset
of physical partitions before scanning rows.

```go
func (r resolver) ResolveSQLSourcePartitionsForPredicate(
	name, key string,
	predicate hatSql.SQLPartitionPredicate,
) ([]hatSql.SQLSourcePartition, bool, error) {
	if predicate.Field != "region" {
		return nil, false, nil
	}
	if predicate.Operator == "=" && len(predicate.Values) == 1 {
		return r.regionPartition(predicate.Values[0])
	}
	if predicate.Operator == "IN" {
		return r.regionPartitions(predicate.Values)
	}
	return nil, false, nil
}
```

## Supported Extraction

- Direct binary equality: `region = 'apac'` and its literal-left form.
- Literal `IN`: `region IN ('apac', 'eu')`.
- Multiple `AND` conjuncts are tried independently until the resolver reports
  one available pruning result.
- `OR`, expressions, parameters that are not resolved literals, `NULL`
  literals, non-binary collations, and indirect fields are left unpruned.
- `TABLESAMPLE` is left unpruned so sampling distribution remains unchanged.
- Unqualified fields are considered only for queries without joins. Qualified
  fields must name the base source alias, avoiding accidental pruning from an
  ambiguous joined field.

The resolver must never omit a partition that could contain a matching row.
The full original `WHERE` expression is still evaluated after pruning, so the
resolver result is only a candidate reduction. Partitions are not deduplicated
and their returned order remains the source order.

The path is used by materialized and stream-oriented scans. Existing global
index and columnar fast paths retain their current precedence. Returning
`available=false` preserves the C141 partitioned path, then the legacy
borrowed or ordinary source path. Resolver errors abort the query rather than
silently falling back to incomplete data.

## Benchmark

Command:

```text
make benchmark-partition-pruning-local-clean
```

Machine: AMD Ryzen 9 5950X, Linux amd64. Five samples per case used the same
query, `WHERE region = 'apac'`, over 2,048 rows in eight partitions. The
pruned case returned the one matching partition; both cases still evaluated
the original predicate.

| Path | Median ns/op | B/op | allocs/op | Relative CPU | Relative bytes | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Partition scan without pruning | 239,486 | 339,665 | 538 | 1.00x | 1.00x | 1.00x |
| Literal equality partition pruning | 76,036 | 135,504 | 537 | 3.15x faster | 2.51x lower | 1.00x |

The benchmark measures a candidate reduction rather than a change to SQL
semantics. The allocation count is nearly unchanged because the selected rows
still become normal SQL result maps; the main gain is avoiding seven unrelated
partition scans and their row-slice flattening.
