# Negative Parse Cache

The prepared SQL cache now memoizes deterministic lexer/parser failures as bounded
negative entries. This is a query-plan admission pattern: repeated malformed
requests do not repeatedly spend CPU in lexical analysis and AST construction.

## Behavior

- Negative entries use the same configured LRU capacity as successful templates.
- The key is the exact source plus schema-version namespace.
- Both lexer errors and parser errors are cached.
- Invalidate and InvalidateSchemaVersion remove negative entries as well as
  successful templates.
- Cache capacity <= 0 preserves the uncached behavior and stores no errors.
- Persistence snapshots contain successful parsed templates only; negative errors
  are intentionally not restored.
- NegativeEntries, NegativeHits, and NegativeAdmissions are exposed by
  SQLPreparedQueryCacheStats.
- Parameter binding and query execution errors are not cached because this cache
  only covers immutable template parsing.

The negative cache is bounded by the existing capacity, so invalid-input storms
cannot create a second unbounded structure. Error values can retain their normal
parser context, but only within that same bounded LRU.

## Measured Result

CPU: AMD Ryzen 9 5950X 16-Core Processor, Linux amd64, five benchmark samples
per variant, go test -benchmem.

| Workload | Baseline median | Negative-cache median | Improvement |
| --- | ---: | ---: | ---: |
| Repeated invalid source, time | 3,558 ns/op | 147.2 ns/op | 24.2x faster |
| Repeated invalid source, memory | 5,592 B/op | 112 B/op | 49.9x lower |
| Repeated invalid source, allocations | 18 allocs/op | 1 alloc/op | 18.0x lower |
| Repeated valid cache hit, time | 1,409 ns/op | 1,414 ns/op | 1.00x, neutral |
| Repeated valid cache hit, memory | 2,488 B/op | 2,488 B/op | 1.00x, neutral |
| Repeated valid cache hit, allocations | 7 allocs/op | 7 allocs/op | 1.00x, neutral |

The invalid-source baseline samples were 3571, 3558, 3560, 3502, 3517
ns/op; after samples were 148.0, 147.2, 148.2, 144.8, 141.9 ns/op.
The valid-source baseline samples were 1473, 1413, 1391, 1409, 1359
ns/op; after samples were 1414, 1465, 1402, 1404, 1416 ns/op.

## Verification

- make test-negative-parse-cache
- make full-test-negative-parse-cache
- make race-negative-parse-cache
- make vet-negative-parse-cache
- make benchmark-negative-parse-cache
