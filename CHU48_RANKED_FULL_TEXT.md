# CH-U48 Ranked Full-Text Search

`TokenPostingsIndex` already supports exact token membership and phrase
matching. `NewRankedTokenPostingsIndex` adds an opt-in BM25-like ranking path
for applications that need relevance ordering as well as filtering.

## Usage

```go
index := hatDataStructure.NewRankedTokenPostingsIndex()
index.Upsert(1, "go go fast")
index.Upsert(2, "go")
index.Upsert(3, "fast")

rows, err := index.MatchRanked("go fast", hatDataStructure.TokenPostingsRankOptions{
	Limit: 2,
})
```

The query is an OR query over normalized Unicode letter/number tokens. The
result is ordered by descending score, then ascending row ID for deterministic
ties:

```text
rows[0].Row == 1  // contains both terms and ranks first
rows[1].Row == 2  // ties with row 3 for this fixture; the lower row ID wins
```

`MatchRankedInto` appends to a caller-owned result slice. A zero limit uses the
default of 100 rows; the hard maximum is 10,000 rows. `K1` defaults to 1.2 and
`B` defaults to 0.75. Invalid options return
`ErrTokenPostingsInvalidRankOptions`.

## Opt-In Cost

The ordinary `NewTokenPostingsIndex` remains memory-minimal and returns
`ErrTokenPostingsRankingDisabled` for non-empty ranked queries. The ranked
constructor retains total normalized document length and a frequency value for
each row-term membership. This increases update/build work and memory, so it
is appropriate only when relevance ordering is needed.

Ranking metadata is derived state, like the postings themselves. Rebuild it
from source documents after persistence restore. `Upsert`, `Delete`, and
`Clear` update or reset the metadata atomically with the postings.

## Benchmark

The benchmark uses 100,000 deterministic documents and a sparse two-token OR
query. Five samples were run with:

```text
make benchmark-chu48-ranked-full-text
```

On an AMD Ryzen 9 5950X, medians were:

| Operation | Median | Allocations | Interpretation |
| --- | ---: | ---: | --- |
| Linear scan | 1,475,217 ns/op | 0 B, 0 allocs | Scans all documents |
| Plain exact postings | 65,635 ns/op | 24,192 B, 24 allocs | Membership only |
| Ranked, limit 10 | 224,278 ns/op | 15,864 B, 46 allocs | 6.58x faster than the scan |
| Ranked, limit 100 | 248,820 ns/op | 24,152 B, 139 allocs | 5.93x faster than the scan |

The ranked limit-10 query is 3.42x slower than plain exact postings because it
must score candidates and maintain a top-K heap. A 20,000-row build measured
21,817,560 ns/op, 17,989,926 B/op, and 120,590 allocs/op for the ranked index;
the plain build measured 16,987,417 ns/op, 14,367,382 B/op, and 100,443
allocs/op. That is 1.28x build time, 1.25x allocated bytes, and 1.20x
allocation count, which is why ranking is not enabled by default.

Raw five-run output is recorded in [BENCHMARK.md](BENCHMARK.md#chu48-ranked-full-text).
