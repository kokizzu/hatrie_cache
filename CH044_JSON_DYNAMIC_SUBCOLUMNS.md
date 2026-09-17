# CH-044 JSON Dynamic Subcolumns

Hatrie cache can optionally promote frequently queried scalar JSON paths into
compact typed subcolumns. This is the ClickHouse-inspired dynamic-subcolumn
idea: repeated `JSON_VALUE`, `JSON_QUERY`, or `JSON_EXISTS` paths can avoid
repeating JSON path extraction for every row.

## Enable

The feature is disabled by default. Enable it explicitly after creating a
`HatTrie`:

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()

err := trie.ConfigureSQLJSONSubcolumnAutoMaterializer(
    hatSql.JSONSubcolumnAutoMaterializerOptions{},
)
```

A zero options value uses these limits:

| Limit | Default |
| --- | ---: |
| Observations before promotion | 3 |
| Promoted paths per trie | 64 |
| Rows per source | 1,048,576 |
| Bytes per promoted path | 64 MiB |

`DisableSQLJSONSubcolumnAutoMaterializer` releases the retained typed-column
cache. `SQLJSONSubcolumnAutoMaterializerStats` reports entries, promotions,
rejections, evictions, hits, and the retained typed-column byte estimate.

## Behavior

- Only `CACHE` sources are eligible.
- Cold paths return `available=false`, so the existing SQL executor handles the
  query unchanged while observations accumulate.
- Cold access observations do not decode source rows until the threshold is
  reached, avoiding duplicate work on the fallback path.
- Promotion supports scalar integer/float, string, and boolean paths.
- Missing paths remain missing; JSON `null` remains present with a null value.
- A write advances the existing per-key source generation. Promoted columns
  from the previous generation are never reused for the new source contents.
- Path-only warm queries use the promoted columns and their stored row count;
  they do not retain or re-decode the full source rows.
- Queries that also project ordinary fields decode only those requested fields
  from the captured source and combine them with the promoted paths.
- Existing SQL source admission limits still apply. Oversized or malformed
  sources fall back to the established path.

Example source:

```json
[
  {"doc":{"user":{"id":7,"name":"ann"}}},
  {"doc":{"user":{"name":"bob"}}},
  {"doc":{"user":{"id":null}}}
]
```

Example query:

```sql
FROM CACHE('users') AS user
WHERE JSON_VALUE(user.doc, '$.user.id') >= 7
SELECT JSON_VALUE(user.doc, '$.user.id') AS id
```

The result contains one row, `id = 7`. A direct subcolumn read returns
`(nil, false)` for the second row because the path is missing and `(nil, true)`
for the third row because the path is explicitly JSON `null`.

## Measurement

Command:

```text
make benchmark-ch044
```

The benchmark executes the same query over 4,096 deterministic JSON documents
and uses five benchmark samples. The disabled `HatTrie` row is the
pre-feature-equivalent ordinary columnar path with the new adapter returning
`available=false`; the materialized case has two paths promoted before timing.

| Path | Median ns/op | B/op | Allocs/op | Retained typed bytes | Relative to disabled |
| --- | ---: | ---: | ---: | ---: | ---: |
| `HatTrie`, materializer disabled | 13,898,674 | 8,821,725 | 118,885 | 0 | 1.00x |
| `HatTrie`, typed subcolumns enabled | 3,291,193 | 2,007,618 | 36,927 | 134,058 | 4.22x faster, 4.39x lower heap, 3.22x fewer allocations |

The retained-byte value is the materializer's typed-column payload estimate,
not a process-wide heap measurement. The integration intentionally does not
populate `sqlJSONIndexSnapshots`; a materializer-only workload therefore does
not retain a second full decoded row set. Ordinary SQL indexes, if configured,
may retain their own snapshots independently.

The row-only control in the raw benchmark was 8,951,998 ns/op, 6,817,396
B/op, and 69,687 allocations/op. It hides the existing ordinary columnar
resolver and is included only to show workload shape; it is not the
pre-feature `HatTrie` baseline.
