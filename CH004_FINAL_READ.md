# CH-004 `FINAL` Read Semantics

`FINAL` is an explicit, query-time reconciliation modifier for sources that
contain replacing or collapsing versions. It is disabled by default. A query
without `FINAL` keeps the existing source rows and execution path.

## Query Syntax

`FINAL` can appear next to a source before or after its alias:

```sql
FROM CACHE('events') FINAL AS event
SELECT event.id, event.value

FROM CACHE('events') AS event FINAL
SELECT event.id, event.value
```

The modifier applies to the source immediately before the next query clause.
It can also be used on a source in a join or a derived source. The query
executor reconciles every marked source before joins, filters, projections, or
aggregates consume it.

## Configuration

Generic `SQLRow` values do not have universal key, version, or sign fields, so
the caller must provide a deterministic source contract:

```go
options := hatSql.SQLQueryOptions{
	FinalSourceOptions: &hatSql.SQLFinalSourceOptionsResolver{
		Resolve: func(kind, key string) (hatSql.SQLFinalOptions, bool, error) {
			if kind != "CACHE" || key != "events" {
				return hatSql.SQLFinalOptions{}, false, nil
			}
			return hatSql.SQLFinalOptions{
				Mode: hatSql.SQLFinalReplacing,
				Key: func(row hatSql.SQLRow) string {
					return row["id"].(string)
				},
				Version: func(row hatSql.SQLRow) (uint64, error) {
					return row["version"].(uint64), nil
				},
			}, true, nil
		},
	},
}

result, err := hatSql.ExecuteSQLQueryContext(ctx, query, resolver, options)
```

Returning `false` means the source is not configured. The query fails with
`ErrSQLFinalOptionsRequired`; it never silently returns unreconciled rows.
Missing or incompatible callbacks fail with `ErrSQLFinalOptionsInvalid`.

## Modes

### Replacing

`SQLFinalReplacing` keeps one row per key. The highest version wins; equal
versions use the later input row. A nil `Version` callback is allowed and
selects the last input row for each key. The output keeps first-seen key order.

### Collapsing

`SQLFinalCollapsing` pairs rows with the same key and opposite `Sign` values.
Only `-1` and `1` are valid signs. Unmatched rows remain visible, which makes
incomplete cancellation explicit instead of silently dropping data.

## Execution and Resource Behavior

- `FINAL` is opt-in and has no effect on legacy queries.
- Without `SQLFinalSourceResolver`, each marked source is materialized before
  reconciliation. The result is cloned, so merge output does not mutate the
  resolver's source maps; a native provider can avoid that materialization.
- Native scalar, index, columnar, ordered, projection-cache, and result-cache
  shortcuts are bypassed for a query containing `FINAL`; this prevents stale
  or unreconciled rows from bypassing the contract.
- `ExecuteSQLQueryRows` still invokes the caller's row callback, but only after
  the marked source has been reconciled. It is not a bounded streaming merge.
- A source may implement `SQLFinalSourceResolver` to reconcile rows in its
  native representation. The engine validates the returned rows and caches
  them separately from raw source rows; `available=false` uses the generic
  materialize-and-merge fallback. See [CH004_FINAL_PUSHDOWN.md](CH004_FINAL_PUSHDOWN.md).
- Callbacks must be deterministic, read-only, and safe for the source rows.
  They are application code and should validate row types instead of using
  unchecked assertions when input is not trusted.
- Persistent schema-bound FINAL metadata, automatic key/version discovery, and
  background merge integration remain future work. The current API requires an
  explicit caller contract for every marked source.

## Measurement

The benchmark uses 4,096 rows, 1,024 logical keys, five samples, and an AMD
Ryzen 9 5950X Linux/amd64 host. The legacy path is the same query without
`FINAL`; multipliers are relative to its median from the comparison run.

| Path | Median ns/op | B/op | Allocs/op | CPU vs legacy | Heap vs legacy | Allocs vs legacy |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Legacy query | 1,360,240 | 1,413,048 | 8,205 | 1.00x | 1.00x | 1.00x |
| Replacing FINAL | 2,201,275 | 2,578,948 | 12,328 | 1.62x | 1.83x | 1.50x |
| Collapsing FINAL | 4,706,292 | 5,829,332 | 27,705 | 3.46x | 4.13x | 3.38x |
| Replacing FINAL row callback | 2,889,301 | 2,762,439 | 18,482 | 2.12x* | 1.95x* | 2.25x* |

The row-callback row is not a strict apples-to-apples comparison because the
legacy control is materialized. It shows the cost of reconciling before
emission, not a claim that callbacks are slower than materialization.

Raw commands:

```text
make benchmark-ch004-final-baseline
make benchmark-ch004-final
make benchmark-ch004-final-rows
```

Raw samples are retained in [BENCHMARK.md](BENCHMARK.md#ch-004-final-read-semantics).
