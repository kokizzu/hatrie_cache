# T225: Covering Indexes For Projected Fields

The round-2 covering-index idea was already implemented by the TR-024
materialized covering-index path. This record refreshes correctness and
performance evidence; it does not add a duplicate runtime implementation.

## Implementations

- `hatSchema.MaterializedSource` can maintain projected fields alongside an
  equality key, so a matching query can return the projection without fetching
  the source tuple.
- The SQL resolver chooses a covering index only when the predicate and
  requested projection are compatible, and otherwise retains the ordinary
  source-fetch fallback.
- Rebuilds replace the covering state atomically, while inserts, replacements,
  and upserts maintain the projected payload.

Detailed API and lifecycle documentation remains in
[TR024_COVERING_INDEX.md](TR024_COVERING_INDEX.md) and
[SQL_COVERING_INDEX.md](SQL_COVERING_INDEX.md).

## Correctness Verification

The focused checks cover refresh after string replacement, projection without
source rows, SQL resolver selection, SQL query execution, upsert-conflict
maintenance, and covering-index verification:

```text
make test-t225
make race-t225
make vet-t225
```

All focused tests, SQL integration checks, verification, race checks, and vet
checks passed before this documentation change.

## Benchmark

Fresh runs used `-benchmem -count=5` on Linux/amd64 with an AMD Ryzen 9 5950X.
The values below are medians from the raw runs recorded in `BENCHMARK.md`.

| Workload | ns/op | bytes/op | allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Source-fetch indexed projection | 760,333 | 567,264 | 1,909 | 1.00x |
| Covering indexed projection | 250,510 | 284,265 | 1,283 | 3.04x faster; 2.00x fewer bytes; 1.49x fewer allocations |

The covering path wins because it avoids fetching and decoding the source row
for the projected result. The measured bytes are per query and do not represent
the retained covering-index footprint.

## Limits and tradeoffs

Covering indexes retain projected payloads, so index memory grows with the
number and width of projected fields. Every source replacement or upsert must
maintain that payload, and a stale or incompatible projection must fall back to
the source path. The feature remains opt-in and is most useful for repeated
read-heavy equality queries with a small stable projection.
