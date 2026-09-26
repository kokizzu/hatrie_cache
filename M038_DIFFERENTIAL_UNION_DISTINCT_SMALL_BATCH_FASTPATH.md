# M038 Differential Union Distinct Small-Batch Fast Path

## Change

`UnionDifferentialRows` now returns two nonzero rows directly when their
`(key,time)` identities differ. It still validates both keys, clones both
rows, drops zero-weight updates, and preserves input order. Same-identity
rows, larger batches, and overflow/consolidation cases retain the existing
reducer path.

## Benchmark

Machine: AMD Ryzen 9 5950X 16-Core Processor, Linux amd64.

Each case was run five times by the repository benchmark target. The table
uses the median of the five samples; raw samples are retained below.

| Case | Before | After | Improvement |
| --- | ---: | ---: | ---: |
| two distinct rows, ns/op | 522.9 | 385.3 | 1.36x faster |
| two distinct rows, B/op | 832 | 752 | 1.11x lower |
| two distinct rows, allocs/op | 6 | 5 | 1 fewer allocation |
| two same-identity rows, ns/op | 212.1 | 213.4 | within benchmark noise |
| two same-identity rows, B/op | 384 | 384 | unchanged |
| two same-identity rows, allocs/op | 3 | 3 | unchanged |

### Raw samples

| Case | Before samples | After samples |
| --- | --- | --- |
| two distinct rows, ns/op | 547.2, 527.4, 519.6, 522.9, 521.6 | 410.3, 383.6, 391.7, 385.3, 385.1 |
| two same-identity rows, ns/op | 209.3, 213.5, 213.0, 212.1, 209.6 | 213.4, 214.3, 211.9, 214.4, 210.4 |

The before and after runs used the same benchmark cases and five-sample
configuration. The same-identity case confirms that duplicate consolidation
remains on its existing path.

## Verification

- Focused correctness tests passed before the implementation change.
- Focused correctness tests passed after the implementation change.
- `go test -race ./hat/hatSql` passed.
- Full `go test ./hat/hatSql` passed.
- Tests cover input/output ownership, order, signed weights, timestamps,
  zero-weight updates, and same-identity consolidation fallback.
