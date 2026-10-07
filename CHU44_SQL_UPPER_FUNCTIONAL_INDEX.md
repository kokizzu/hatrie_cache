# SQL `UPPER(...)` Functional Index

Hatrie Cache now supports an opt-in SQL expression index for homogeneous text
fields when queries normalize a value with `UPPER(...)`. This extends the
existing `LOWER(...)` functional-index path without enabling arbitrary callback
execution inside the SQL planner.

## Configure

```go
if err := trie.CreateSQLJSONUpperIndex("people", "name"); err != nil {
    return err
}
```

The index is used for equality and literal `IN` predicates:

```sql
FROM CACHE('people') AS person
WHERE UPPER(person.name) = 'ADA'
SELECT person.id

FROM CACHE('people') AS person
WHERE UPPER(person.name) IN ('ADA', 'GRACE')
SELECT person.id
```

The executor still evaluates the original predicate on candidate rows before
returning them. Index source generations are refreshed after replacement, and
`CheckSQLJSONIndexConsistency` reports the index as kind `upper`.

## Admission And Fallback

The index is deliberately opt-in and is not built by default. A source is
indexed only when its non-null values for the configured field are text. If a
source contains mixed types, the resolver declines the index and the normal
scan path preserves the existing type/error semantics. Null values also retain
normal SQL behavior.

## Query Benchmark

This is a steady-state query benchmark, not an index-build benchmark. It uses
10,000 JSON rows, 100 matching rows, setup outside the timer, five repetitions,
and `-benchtime=100ms` on an AMD Ryzen 9 5950X, linux/amd64.

| Path | Median ns/op | Median B/op | Median allocs/op | Scan / indexed |
| --- | ---: | ---: | ---: | ---: |
| Full scan | 12,486,278 | 7,841,648 | 140,248 | 1.00x |
| `UPPER(...)` index | 75,574 | 112,264 | 735 | 165.2x time, 69.8x bytes, 190.8x allocations |

Raw result:

```text
BenchmarkSQLJSONUpperIndexQuery/scan-32         9  12375958 ns/op  7841640 B/op  140248 allocs/op
BenchmarkSQLJSONUpperIndexQuery/scan-32         9  12350379 ns/op  7841636 B/op  140248 allocs/op
BenchmarkSQLJSONUpperIndexQuery/scan-32         9  12964373 ns/op  7841662 B/op  140248 allocs/op
BenchmarkSQLJSONUpperIndexQuery/scan-32         9  12486278 ns/op  7842257 B/op  140249 allocs/op
BenchmarkSQLJSONUpperIndexQuery/scan-32         8  13736770 ns/op  7841648 B/op  140248 allocs/op
BenchmarkSQLJSONUpperIndexQuery/indexed-32   1377     75574 ns/op   112264 B/op     735 allocs/op
BenchmarkSQLJSONUpperIndexQuery/indexed-32   1656     77244 ns/op   112264 B/op     735 allocs/op
BenchmarkSQLJSONUpperIndexQuery/indexed-32   1602     71485 ns/op   112264 B/op     735 allocs/op
BenchmarkSQLJSONUpperIndexQuery/indexed-32   1610     73671 ns/op   112264 B/op     735 allocs/op
BenchmarkSQLJSONUpperIndexQuery/indexed-32   1382     77035 ns/op   112264 B/op     735 allocs/op
```

## Tradeoff

The query path is substantially cheaper, but the index retains its posting
rows and adds rebuild work when the source changes. Build time and retained
index memory are intentionally outside the steady-state table above; callers
should enable the index only for repeated normalized-text predicates where the
query savings justify that retained storage. Existing defaults and storage
formats are unchanged.
