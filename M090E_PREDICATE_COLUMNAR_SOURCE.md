# M090E: Predicate-Aware Columnar Source Resolution

This is the next slice of the Materialize-style independent compute/storage
scaling idea (M090). The existing columnar SQL path already avoided row-map
construction, but a remote columnar resolver still had to transfer every row
before the executor applied a literal `WHERE` predicate.

## What Changed

`hatSql.PredicateColumnarSourceResolver` is an opt-in contract:

```go
type PredicateColumnarSourceResolver interface {
    ResolveSQLColumnarSourceWithPredicates(
        name, key string,
        fields []string,
        predicates []SQLPartitionPredicate,
    ) (ColumnarBatch, bool, error)
}
```

For a supported single-source query, the executor passes the projected fields
and planner-proven literal predicates to the resolver. The resolver may prune
rows in storage and return a smaller `ColumnarBatch`. The executor still runs
the original SQL expression over that batch, so a resolver may return
conservative false positives but must never omit a row that could match.

Returning `available=false`, implementing the old interface only, or querying a
source without extractable literal predicates keeps the existing parts,
borrowed, or full-columnar path unchanged. `SQLSession` and `CatalogResolver`
forward the opt-in contract while preserving local/session and virtual catalog
source precedence.

## Benchmark

Command:

```text
make benchmark-m090e-predicate-columnar-source
```

Environment: Linux amd64, AMD Ryzen 9 5950X, Go benchmark `-benchtime=100ms
-count=5`, 4,096 rows, alternating `eu`/`us`, query selects `id` with
`WHERE region = 'eu'`. The source-byte metric counts the selected `id` and
`region` values crossing the resolver boundary; it intentionally excludes
unselected payload columns.

| Path | ns/op samples | B/op samples | allocs/op samples | source bytes/op | source rows/op |
| --- | --- | --- | ---: | ---: | ---: |
| legacy full columnar | 543963, 581562, 558363, 597561, 566765 | 873243, 873212, 873211, 873213, 873212 | 4136, 4136, 4136, 4136, 4136 | 73728 | 4096 |
| predicate columnar | 551911, 527799, 510494, 549959, 541027 | 840849, 840844, 840838, 840851, 840843 | 4142, 4142, 4142, 4142, 4142 | 36864 | 2048 |

Median comparison:

| Metric | Legacy | Predicate-aware | Improvement |
| --- | ---: | ---: | ---: |
| query time | 566,765 ns/op | 541,027 ns/op | 1.05x faster |
| heap bytes | 873,212 B/op | 840,843 B/op | 1.04x lower |
| allocations | 4,136 | 4,142 | 1.00x, six more |
| source transfer proxy | 73,728 B/op | 36,864 B/op | 2.00x lower |
| source rows transferred | 4,096 | 2,048 | 2.00x lower |

The CPU win is workload- and resolver-dependent; the primary benefit is
avoiding remote row transfer and storage-side materialization for rows that the
literal predicate rejects. The six extra allocations are the bounded
predicate/candidate path and are an explicit tradeoff of this opt-in API. The
legacy path does not construct predicate metadata unless the resolver advertises
the new contract.

## Verification

```text
make test-m090e-predicate-columnar-source
make test-m090e-predicate-columnar-source-package
make race-m090e-predicate-columnar-source
make vet-m090e-predicate-columnar-source
make format-m090e-predicate-columnar-source
```

The focused tests cover result correctness, predicate and field forwarding,
legacy fallback, and forwarding through `SQLSession`.
