# Compiled Negative Cache

The immutable compiled SQL-plan cache now memoizes deterministic compilation
failures in the same bounded LRU as successful plans. Repeated malformed
requests avoid repeating token normalization and compilation work.

## Behavior

- Negative entries are keyed by exact source and schema-version namespace.
- Negative entries share both MaxEntries and MaxBytes with successful plans.
- The cache uses a conservative source-based weight; oversized failures are not
  admitted.
- Invalidate and InvalidateSchemaVersion remove negative entries as well as
  compiled plans.
- Persistence is not involved: compiled-plan cache state is process-local.
- NegativeEntries, NegativeHits, and NegativeAdmissions are exposed in
  SQLCompiledQueryCacheStats.
- A failed compilation never produces a compiled handle or increments the
  successful-plan miss counter.

## Measured Result

CPU: AMD Ryzen 9 5950X 16-Core Processor, Linux amd64, five benchmark samples
per variant, go test -benchmem.

| Workload | Baseline median | Compiled-negative median | Improvement |
| --- | ---: | ---: | ---: |
| Repeated invalid source, time | 3,467 ns/op | 25.80 ns/op | 134.4x faster |
| Repeated invalid source, memory | 5,736 B/op | 0 B/op | eliminated |
| Repeated invalid source, allocations | 20 allocs/op | 0 allocs/op | eliminated |
| Repeated valid compiled hit, time | 22.68 ns/op | 22.88 ns/op | 0.99x, noise-level |
| Repeated valid compiled hit, memory | 0 B/op | 0 B/op | 1.00x neutral |
| Repeated valid compiled hit, allocations | 0 allocs/op | 0 allocs/op | 1.00x neutral |

The invalid-source baseline samples were 3413, 3394, 3467, 3482, 3562
ns/op; after samples were 26.12, 25.80, 25.75, 26.09, 25.77 ns/op.
The valid-source baseline samples were 22.89, 21.65, 22.11, 22.92, 22.68
ns/op; after samples were 22.85, 22.89, 23.05, 22.88, 21.97 ns/op.

## Verification

- make test-compiled-negative-cache
- make full-test-compiled-negative-cache
- make race-compiled-negative-cache
- make vet-compiled-negative-cache
- make benchmark-compiled-negative-cache
