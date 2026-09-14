# CH-025 Token Postings Index

This is an opt-in public data structure inspired by ClickHouse inverted and
skip-index designs. It maps normalized text tokens to sorted `uint32` row IDs
using Roaring bitmaps. It is useful when an application repeatedly filters a
large row set by exact tokens and can maintain a derived index beside its
source data.

## API

```go
index := hatDataStructure.NewTokenPostingsIndex()
index.Upsert(10, "Go fast, low-memory")
index.Upsert(20, "Fast query execution")
index.Upsert(30, "memory efficient query")

fmt.Println(index.RowsForToken("FAST"))
// [10 20]
fmt.Println(index.MatchAll("fast query"))
// [20]
fmt.Println(index.MatchAny("low efficient"))
// [10 30]
```

`VisitToken` streams row IDs in ascending order without materializing a result
slice. Returning `false` from the callback stops the visit. `RowsForTokenInto`,
`MatchAllInto`, and `MatchAnyInto` append to a caller-owned buffer.

## Semantics

- Tokens are Unicode letters and digits, lower-cased; punctuation and
  whitespace separate tokens.
- Duplicate tokens in one row are indexed once.
- `Upsert` replaces all old memberships for the row. Empty or tokenless text
  removes the row.
- `MatchAll` uses the smallest posting as its candidate set and checks the
  remaining postings. `MatchAny` builds an exact union.
- Results are exact, sorted, and use `uint32` row IDs.
- The zero value is usable. The type is safe for concurrent readers and
  writers; a `VisitToken` callback must not call back into the same index.

This index handles exact token predicates and token conjunction/disjunction. It
does not implement substring, prefix, stemming, scoring, or positional phrase
matching. The existing SQL/JSON position index remains the appropriate path for
phrase matching. The index is not automatically attached to SQL planning.

## Persistence And Memory

The index is derived state and should be rebuilt from source rows after restore.
`Info.EncodedBytes` reports only the Roaring posting payload, not Go map,
string, slice, allocator, or synchronization overhead. The row-to-term ID map
keeps source document strings out of the index, while active term IDs are
reclaimed when their last row is removed.

## Benchmark

Command:

```text
make benchmark-ch025-token-postings
```

The workload uses 100,000 deterministic documents and five benchmark samples;
the build case indexes 20,000 of those documents. Values below are medians
from the final run on the repository benchmark host. The baseline scans all
100,000 documents and performs no allocations. `x faster` is baseline divided
by indexed time.

| Operation | Linear scan | Token postings | Improvement | Indexed allocations |
| --- | ---: | ---: | ---: | ---: |
| One token, returned rows | 841,568 ns/op | 3,848 ns/op | 218.7x | 4,864 B, 1 alloc |
| One token, callback visit | 841,568 ns/op | 1,855 ns/op | 453.5x | 0 B, 0 alloc |
| All tokens in query | 841,568 ns/op | 10,467 ns/op | 80.4x | 4,992 B, 2 allocs |
| Any token in query | 1,542,656 ns/op | 60,552 ns/op | 25.5x | 24,192 B, 24 allocs |

Index construction measured `17.0 ms/op`, `14.37 MB/op`, and `100,443`
allocations per 20,000-row rebuild. Its measured Roaring posting payload for
that index was `73,632` bytes; this deliberately excludes the Go container
overhead described above. Build and update cost therefore makes this an
explicit opt-in optimization, not a default for every table.
