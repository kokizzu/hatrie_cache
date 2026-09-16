# CH-U13 Phrase Postings

`TokenPostingsIndex` normally indexes a sorted, unique token set. That is the
right default for token predicates because it does not retain token order.

Use `NewTokenPostingsIndexWithPhrases` when exact contiguous phrase predicates
are needed:

```go
index := hatDataStructure.NewTokenPostingsIndexWithPhrases()
index.Upsert(1, "Quick, brown fox")
index.Upsert(2, "quick fox brown")
index.Upsert(3, "the quick brown fox")

index.MatchAll("quick brown fox")   // [1 2 3]
index.MatchPhrase("quick brown fox") // [1 3]
index.MatchPhrase("brown fox")       // [1 3]
```

Matching uses the same Unicode letter/digit tokenization and lower-casing as
the base index. Punctuation and whitespace separate tokens. A phrase may be
embedded in a row; it does not have to start at the first token. Repeated
tokens are significant, so `"quick brown fox"` does not match
`"quick brown brown fox"`.

## API

- `NewTokenPostingsIndexWithPhrases` creates the opt-in variant.
- `MatchPhrase` returns ascending row IDs.
- `MatchPhraseInto` appends to a caller-owned destination slice.
- `VisitPhrase` visits rows without materializing the result slice.
- `PhraseInfo` reports rows, token count, distinct bigram count, encoded
  sequence bytes, and encoded bigram-posting bytes.

`NewTokenPostingsIndex` remains the default. On that variant, phrase methods
are no-ops and return the caller's destination unchanged. The default path
does not allocate phrase maps or ordered row sequences.

## Design

The phrase-enabled variant keeps the existing term-to-row roaring postings and
adds two derived structures:

1. Each row's ordered term-ID sequence is encoded with unsigned varints.
2. Each adjacent term-ID pair has a roaring candidate posting.

The query chooses the smallest adjacent-pair posting, then verifies the full
encoded phrase at token boundaries. The verification step makes pair postings
only a candidate filter; it preserves exactness for prefixes, suffixes,
repeated tokens, and hash-free term-ID pairs. Updates, deletes, `Clear`, and
term-ID reuse remove and rebuild both forms under the index lock.

The index is derived state. As with the base token index, rebuild it from
source rows after persistence restore; phrase state is not a separate durable
source of truth.

Ranking and BM25-style scores are intentionally not included in CH-U13. The
current capability is exact phrase filtering; ranked token postings remain a
separate future feature.

## Measured Tradeoff

Runs used Go benchmarks with `-benchmem -benchtime=100ms -count=5` on Linux,
amd64, AMD Ryzen 9 5950X. The fixture has 20,000 rows and 168,000 tokens.
Values below are medians of the five samples; raw samples are recorded in
`BENCHMARK.md`.

| Operation | Before | After | Relative result | Allocation result |
| --- | ---: | ---: | ---: | ---: |
| Default `MatchAll` | 240,874 ns/op | 262,988 ns/op | 0.92x, 8.4% slower and noisy | 65,728 B/op, 3 allocs/op on both |
| Default update `Upsert` | 935.1 ns/op | 975.5 ns/op | 0.96x, 4.3% slower and noisy | 252 B/op, 11 allocs/op on both |
| Phrase-enabled `MatchAll` | n/a | 267,741 ns/op | same base operation with opt-in state | 65,728 B/op, 3 allocs/op |
| Exact `MatchPhrase` | n/a | 460,549 ns/op | 1.75x slower than after `MatchAll` | 49,280 B/op, 2 allocs/op |
| Phrase-enabled update `Upsert` | n/a | 2,323 ns/op | 2.38x slower than after default update | 780 B/op, 27 allocs/op |

Retained payload accounting for the same fixture:

| Payload | Bytes |
| --- | ---: |
| Default token roaring payload | 73,536 |
| Phrase ordered sequence payload | 168,000 |
| Phrase bigram roaring payload | 105,344 |
| Additional tracked phrase payload | 273,344 |
| Base plus tracked phrase payload | 346,880 |

These counters exclude Go map, slice-header, allocator, and term-string
overhead. The phrase-enabled index therefore uses materially more memory, even
after varint compression. The feature is appropriate when exact phrase
semantics are worth that cost; it is not enabled by default and should not be
used merely to accelerate ordinary token predicates.
