# SQL Text Prefix Index

`CreateSQLJSONTextIndex` now supports the opt-in `CONTAINS_PREFIX` predicate
for one JSON string field. It is a Tarantool-style full-text index extension
for prefix lookup, while preserving the existing exact-token `CONTAINS`
semantics.

```go
trie := hatCache.CreateHatTrie()
defer trie.Destroy()
trie.UpsertString("articles", `[
  {"id":1,"body":"Fast cache indexes for Go"},
  {"id":2,"body":"A slow database migration"},
  {"id":3,"body":"Go query planning and cache design"}
]`)
if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
    panic(err)
}
```

```sql
FROM CACHE('articles') AS article
WHERE CONTAINS_PREFIX(article.body, 'cac')
SELECT article.id
ORDER BY article.id
```

Result:

```text
1
3
```

Prefix input is normalized using the same lowercase Unicode letter/number
tokenization as `CONTAINS`. The function requires exactly one normalized token;
an empty or multi-token prefix matches no rows. A missing text index uses the
ordinary scan path, and indexed candidates are rechecked by the executor
before they are returned. `CONTAINS` continues to mean exact AND-token
containment.

The index keeps the existing token-to-row postings and adds a sorted slice of
token keys. Lookup uses binary search to find the prefix range, unions its
postings, and restores source-row order. The added slice stores one pointer per
distinct token and does not duplicate token strings. Token-position phrase
search is still deferred.

## Measurement

`make benchmark-tt024-text-prefix` ran the same 10,000-row query five times for
the ordinary scan and the indexed prefix path. The selective prefix matched 100
rows and the field had five distinct tokens.

| Path | Median CPU | Median transient bytes | Median allocations |
| --- | ---: | ---: | ---: |
| Full scan | 15,348,738 ns/op | 8,906,138 B/op | 160,237 allocs/op |
| Sorted token postings | 71,786 ns/op | 82,568 B/op | 726 allocs/op |
| Indexed improvement | 213.8x faster | 107.9x lower | 220.7x fewer |

The byte and allocation figures are per-query transient work, not the retained
index footprint. The index build/refresh cost is paid when the source
generation changes. The benchmark's five-token fixture adds five token-key
slice entries; larger dictionaries add one slice entry per distinct token.

Raw samples:

```text
scan:
15106885 8906136 160237
15523546 8906138 160237
15083938 8906135 160237
15348738 8906210 160237
15374864 8906138 160237

sorted_token_postings:
73769 82568 726
73721 82568 726
69037 82568 726
71786 82568 726
71033 82568 726
```
