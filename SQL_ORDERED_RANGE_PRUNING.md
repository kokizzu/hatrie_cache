# Ordered SQL Range Pruning

HAT-trie now supports a narrow, safe subset of ClickHouse-style primary-key
range pruning for indexed JSON sources. It uses the ordered slice already built
by a generic `CreateSQLJSONFieldIndex` or typed `CreateSQLTypedJSONIndex`; it does
not add a second per-row index or physical part metadata.

## What It Does

For a single-source query with one indexed `ORDER BY` field, the planner can
recognize one literal comparison on that same field:

```sql
FROM CACHE('events') AS event
WHERE event.score >= 90000
ORDER BY event.score
LIMIT 10
SELECT event.id, event.score
```

The resolver binary-searches the ordered index and returns or streams only the
candidate range. The SQL executor still evaluates the complete original
`WHERE`, `OFFSET`, `LIMIT`, projection, row limits, and result-byte limits. This
keeps the optimization an input reduction rather than a change to SQL meaning.

Supported comparisons are `=`, `<`, `<=`, `>`, and `>=`. A comparison may be a
conjunct inside `AND`; the other conjuncts are still evaluated normally.

Missing and JSON `null` values are excluded from a comparison range because
they cannot satisfy the comparison. Stable source order is retained for equal
ordered values, including descending scans.

## Index Setup

The generic index requires no type declaration:

```go
trie.UpsertString("events", `[
  {"id":1,"score":10},
  {"id":2,"score":20},
  {"id":3,"score":20},
  {"id":4,"score":30}
]`)
if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
    return err
}
```

For exact integral values, the compact typed index is also supported:

```go
if err := trie.CreateSQLTypedJSONIndex(hatCache.SQLJSONIndexSpec{
    CacheKey: "events",
    Fields:   []string{"score"},
    Type:     hatCache.SQLIndexInt64,
}); err != nil {
    return err
}
```

The typed index is opt-in and rejects mixed representations instead of silently
changing their meaning. Index admission, refresh, and ordinary ordered-index
fallback rules remain unchanged.

## Resolver Contracts

`hatSql.OrderedRangeSourceResolver` adds:

```go
ResolveSQLOrderedSourceRange(
    name, key, field string,
    desc, nullsFirst, nullsLast bool,
    operator string, value interface{},
) ([]Row, bool, error)
```

`hatSql.OrderedRangeStreamSourceResolver` is the callback-based equivalent.
Resolvers return `available=false` without candidates when they cannot prove the
range. Session and monitoring wrappers forward both optional contracts. A
resolver may implement only the new contract, but the normal `OrderedSource`
fallback remains responsible for unsupported queries.

The optimizer deliberately declines joins, unions, expressions on the ordered
field, parameter shapes it cannot prove, `OR`, and `NOT` forms. Those queries
continue through the previous ordered or general plan. The complete predicate
is also retained after pruning, so an incomplete or approximate index cannot
produce false positives in the result.

## Measurements

The five-run benchmarks used `make benchmark-ch002-hattrie-materialized` and
`make benchmark-ch002-hattrie-stream` on an AMD Ryzen 9 5950X. Each workload
contains 100,000 ordered JSON rows, selects `score >= 90,000`, and returns ten
rows. The baseline wrapper exposes the pre-existing full ordered resolver but
hides the new range method; the optimized side uses the real `HatTrie` generic
JSON index. `B/op` is Go allocation volume per operation, not retained RSS.

### Materialized API

| Run | Legacy ordered scan | Ordered range pruning | Improvement |
| --- | --- | --- | --- |
| ns/op | 128510420; 129374781; 127066067; 127924425; 127461277 | 7783891; 7693653; 7693180; 7723676; 7657705 | 16.63x faster at median |
| B/op | 77566067; 77565375; 77565391; 77565405; 77565434 | 10982128; 10968880; 10982122; 11041518; 10966337 | 7.06x lower |
| allocs/op | 1530053; 1530052; 1530051; 1530052; 1530052 | 88595; 88323; 88595; 89818; 88270 | 17.27x fewer |

Medians: 127,924,425 ns/op versus 7,693,653 ns/op; 77,565,405 B/op versus
10,982,122 B/op; and 1,530,052 allocations versus 88,595.

### `QueryRows` streaming API

| Run | Legacy ordered scan | Ordered range pruning | Improvement |
| --- | --- | --- | --- |
| ns/op | 47902110; 43011596; 43621677; 43376314; 43888450 | 11906; 12017; 11528; 11622; 11885 | 3,670x faster at median |
| B/op | 57367136; 57261784; 57164563; 57164558; 57164573 | 14394; 14407; 14384; 14397; 14416 | 3,971x lower |
| allocs/op | 504289; 502121; 500120; 500120; 500120 | 104; 104; 104; 104; 105 | 4,809x fewer |

Medians: 43,621,677 ns/op versus 11,885 ns/op; 57,164,573 B/op versus
14,397 B/op; and 500,120 allocations versus 104.

The streaming result is especially large because the ordered range is binary
searched and the LIMIT callback stops after ten matching rows. The materialized
Top-N path still examines the selected 10,000-row range to preserve arbitrary
filter and ordering behavior.

## Verification

The focused checks are:

```text
make test-ch002-primary-mark-pruning
make test-ch002-real-index
```

They compare old and new resolver results, cover generic and typed indexes,
check public materialized and streaming APIs, verify duplicate order and
NULL/missing behavior, and prove an unsafe `OR` predicate falls back without
calling range pruning.

This is a partial ClickHouse adoption. HAT-trie does not yet maintain a
physical part directory, sparse mark files, or mark-level storage statistics;
those would be a separate storage design and are intentionally not implied by
this query-level optimization.
