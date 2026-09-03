# SQL Keyset Pagination

Keyset pagination is an explicit alternative to the existing offset cursor for
deep pages of one-field ordered `CACHE` queries. It asks the source index to
start after the last ordered row instead of scanning and discarding every
earlier row.

The existing offset API remains the default and is unchanged:

```go
result, err := hatCache.ExecuteSQLQueryPage(ctx, query, trie, nil, options, 100, cursor)
```

Use keyset pagination when the query has one direct ordered `CACHE` source and
the source has a compatible generic JSON field index or typed `SQLIndexInt64`
index:

```go
result, err := hatCache.ExecuteSQLQueryKeysetPage(
	ctx,
	"SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score",
	trie,
	nil,
	hatCache.SQLQueryOptions{},
	100,
	"",
)
next, err := hatCache.ExecuteSQLQueryKeysetPage(ctx, "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score", trie, nil, hatCache.SQLQueryOptions{}, 100, result.NextCursor)
```

The HTTP request uses the same opaque cursor with an explicit `keyset` flag:

```json
{
  "query": "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score",
  "page_size": 100,
  "keyset": true,
  "cursor": ""
}
```

The Go HTTP client exposes the corresponding convenience method:

```go
result, err := conn.QueryKeysetPage(ctx, query, nil, 100, cursor)
```

## Contract

- `keyset` is opt-in. An omitted flag continues to use offset pagination.
- `keyset` cannot be combined with streaming, `EXPLAIN`, joins, grouping,
  `DISTINCT`, windows, unions, CTEs, aggregates, `OFFSET`, or a multi-field
  order.
- The ordered field must be qualified by the direct source alias and selected
  output expressions must be stream-compatible.
- A compatible source must implement
  `KeysetOrderedStreamSourceResolver`. Sources without it return an explicit
  error rather than silently changing to offset behavior.
- Cursors are bound to the query text and parameters. They are opaque and must
  be treated as untrusted input.
- The source returns a stable order value and tie position. Duplicate order
  values retain source order across pages. Stale or out-of-range positions are
  rejected.
- Existing source snapshot locking and result/resource limits still apply.

The current HatTrie adapter supports generic JSON field indexes and typed
`INT64` indexes. It does not yet keyset-seek composite, text, bitmap, covering,
or columnar projection indexes; those queries retain the established APIs.

## Benchmark

Command:

```text
make benchmark-sql-keyset-hattrie
```

Workload: 100,000 JSON rows, a warmed `score` field index, page size 100, and
page 900 (offset 90,000). The measured operation fetches the deep page after
the cursor has already been prepared. Five samples were run on Linux with an
AMD Ryzen 9 5950X.

Raw output:

| Path | Time (ns/op) | Heap (B/op) | Allocations (allocs/op) |
| --- | ---: | ---: | ---: |
| Offset | 46,637,998; 52,593,656; 47,095,340; 48,926,760; 48,599,445 | 86,430,559; 86,431,636; 86,430,888; 86,431,157; 86,430,585 | 400,062; 400,064; 400,063; 400,062; 400,062 |
| Keyset | 57,857; 57,385; 59,517; 57,238; 58,087 | 103,830; 103,830; 103,829; 103,830; 103,830 | 741; 741; 741; 741; 741 |

Median summary:

| Path | Median time | Median heap | Median allocations | Relative to offset |
| --- | ---: | ---: | ---: | ---: |
| Offset cursor | 48.60 ms | 86.43 MB | 400,062 | 1.00x |
| Keyset cursor | 57.86 us | 103.83 KB | 741 | 840x lower time, 832x lower heap, 540x fewer allocations |

This is a deep-page read-cost comparison, not a claim that keyset is faster for
every workload. Offset pagination is still useful for random page numbers and
backward compatibility. Keyset cursors are the lower-cost choice when callers
walk pages in order.
