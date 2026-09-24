# MZ-025 Arrangement Sharing

## Change

`TypedTableAggregateArrangements` now canonicalizes the ordered `GROUP BY`,
aggregate-field, distinct-field, and dictionary-encoding options. It uses a
`uint64` FNV-1a hash to select a bucket and compares the canonical definition
inside that bucket before sharing state.

The old serialized string key is retained only for deterministic diagnostics,
snapshot ordering, recovery duplicate detection, and advisor compatibility.
The repeated `Acquire` path does not allocate that string.

## Correctness

Equivalent definitions with surrounding whitespace share one aggregate. A
different field or option does not share it. Hash collisions are handled by the
canonical-definition comparison, so they cannot merge incompatible aggregates.
Recovery, snapshots, statistics, advisor ordering, and checkpoint validation
continue to use the same logical definition semantics.

Verification:

- `make test-mz025-arrangement-sharing`
- `make verify-mz025-arrangement-sharing`
- `make benchmark-mz025-arrangement-sharing`

`verify-mz025-arrangement-sharing` runs the full `hatSql` package tests, the
focused race test, and `go vet ./hat/hatSql`.

## Benchmark

Command: `make benchmark-mz025-arrangement-sharing` with
`-benchtime=100x -count=5`.

| Path | Raw samples (ns/op) | Median | Heap | Allocations |
| --- | ---: | ---: | ---: | ---: |
| Existing serialized string key | 302.4, 244.0, 254.0, 250.4, 193.7 | 254.0 | 104 B/op | 4 allocs/op |
| Canonical numeric hash bucket | 277.0, 174.6, 202.3, 214.7, 281.5 | 214.7 | 64 B/op | 2 allocs/op |

Observed result: approximately `1.18x` faster, `38%` less allocated memory,
and `2x` fewer allocations for this repeated acquire/release workload.

## Scope And Tradeoff

This optimizes arrangement registry lookup. It does not automatically create
arrangements for arbitrary SQL plans, and it does not change aggregate update,
hydration, persistence, or query execution behavior. The registry stores a
canonical definition per live entry and scans only the rare hash-collision
bucket; the extra collision-safe metadata is the small memory cost exchanged
for lower repeated lookup allocation.
