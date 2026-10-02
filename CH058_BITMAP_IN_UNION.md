# CH-058 Bitmap-Backed Literal `IN` Union

This feature combines a literal SQL `IN` list with one low-cardinality bitmap
index lookup. It is inspired by ClickHouse set-oriented `IN` execution and
Tarantool bitmap membership indexes.

## Behavior

`WHERE row.field IN (...)` remains opt-in through the existing
`CreateSQLJSONBitmapIndex`. For a binary-collation literal list, the SQL
executor now asks an optional `SQLMultiValueIndexedSourceResolver` to resolve
all values in one call. `HatTrie` implements that contract for bitmap indexes:

- one source snapshot and index refresh check covers the full list;
- duplicate and `NULL` literals are ignored with existing SQL semantics;
- postings are visited directly without materializing one ordinal slice per
  literal;
- candidate rows are cloned once, then the executor reevaluates the predicate;
- non-bitmap indexes and sources fall back to the existing per-value path.

No new index storage or configuration is required. Bitmap indexes remain
explicit and disabled unless a caller creates one.

## Measurement

Fixture: 4,000 JSON rows, eight low-cardinality `state` values, four literal
values, Linux/amd64, AMD Ryzen 9 5950X, `-benchtime=200ms -count=3`.

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Legacy per-value equality resolver | 429,240 | 716,283 | 4,101 | baseline |
| Batched bitmap `IN` resolver | 422,634 | 708,109 | 4,091 | 1.02x faster, 1.01x lower bytes, 10 fewer allocations |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#ch-058-bitmap-backed-literal-in-union).
The gain is intentionally reported as modest: row-map cloning still dominates
this read-heavy fixture. The batching path has no correctness or storage
tradeoff and retains the old behavior when a bitmap index is unavailable.

## Verification

```text
make test-ch058-bitmap-in
make benchmark-ch058-bitmap-in
```
