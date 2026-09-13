# SQL Text Phrase And Proximity Search

Hatrie supports exact phrase and ordered proximity predicates for text fields:

```sql
CONTAINS_PHRASE(article.body, 'quick brown fox')
CONTAINS_PROXIMITY(article.body, 'quick fox', 2)
```

`CONTAINS_PHRASE` requires consecutive normalized tokens. `CONTAINS_PROXIMITY`
requires the query tokens in order and treats its third argument as the
maximum number of intervening tokens between consecutive query tokens. Thus,
`2` matches both `quick brown fox` and `quick red brown fox`, while `1` matches
only the first example. Repeated query tokens are significant.

Token normalization is shared with the existing text predicates: text is
lowercased, Unicode letters and numbers form tokens, and other characters are
separators. Existing `CONTAINS` behavior is unchanged; it remains an unordered
distinct-token test, while `CONTAINS_PREFIX` remains a single-token prefix test.

## Indexed Use

Create the ordinary opt-in text index once for the JSON field:

```go
trie := hatCache.CreateHatTrie()
trie.UpsertString("articles", `[
  {"id": 1, "body": "Quick brown fox"},
  {"id": 2, "body": "Quick red brown fox"},
  {"id": 3, "body": "Fox brown quick"}
]`)
if err := trie.CreateSQLJSONTextIndex("articles", "body"); err != nil {
    panic(err)
}
```

Then query it normally:

```sql
FROM CACHE('articles') AS article
WHERE CONTAINS_PHRASE(article.body, 'quick brown')
SELECT article.id
ORDER BY article.id
```

The result is:

```text
id
--
1
```

With proximity:

```sql
FROM CACHE('articles') AS article
WHERE CONTAINS_PROXIMITY(article.body, 'quick fox', 2)
SELECT article.id
ORDER BY article.id
```

The result is:

```text
id
--
1
2
```

The optimized path applies to a direct indexed field with a literal query and,
for proximity, a literal non-negative integer distance. Other valid SQL
expressions use the normal evaluator. A missing index also uses the normal
full scan, so adding these functions does not require a schema change.

## Memory And Refresh

The existing token postings remain the admission and candidate source. A
positional sidecar is built lazily only when a phrase or proximity query first
needs it. It stores two `uint32` values per token occurrence: source row and
token position. The sidecar is rebuilt when the indexed source snapshot changes;
ordinary text-index refresh behavior is preserved.

The resolver chooses the rarest query token to limit candidate rows, checks the
ordered positions with binary search, and returns candidates in source order.
The SQL executor evaluates the complete predicate again before returning rows,
which preserves correctness if a resolver is conservative.

The proximity distance must be an integer from `0` through `1,048,576`.
Negative, fractional, non-finite, or larger values return an error instead of
creating unbounded work. `NULL` text, query, or distance follows SQL NULL
behavior; non-text first or second arguments return a type error.

## Measurement

On the repository's AMD Ryzen 9 5950X Linux/amd64 host, a 20,000-row fixture
with 40 phrase matches measured a median `6,347,532 ns/op`, `2,560,004 B/op`,
and `40,000 allocs/op` for the full-scan matcher. The warm indexed SQL query
measured `38,063 ns/op`, `44,088 B/op`, and `306 allocs/op`: `166.7x` faster,
`58.1x` lower transient allocation volume, and `130.7x` fewer allocations.
The fixture retained 100,000 positional postings, or 800,000 bytes of raw
`uint32` payload before map and slice overhead. See the raw samples and
reproduction target in [BENCHMARK.md](BENCHMARK.md#ch-026-sql-phrase-and-proximity-search).
