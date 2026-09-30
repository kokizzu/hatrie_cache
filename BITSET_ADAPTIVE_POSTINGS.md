# Adaptive Bitset Index Postings

This change applies a ClickHouse-style small-cardinality representation to the
typed `BitsetIndex`, with the same motivation as compact secondary-index
postings in Tarantool. A key with one indexed slot is stored as an inline
`uint32`; when a second slot arrives, the posting promotes to the existing
dense bitmap. Removing one of two slots demotes it back to the inline form.

The public API and sorted lookup order are unchanged. The fixed slot arrays
used for exact membership remain unchanged, and multi-slot postings still use
the original bitmap scan. Only the per-key posting representation changes.

## Measurement

Command: `make benchmark-bitset-adaptive`

Workload: five `-benchmem` samples on an AMD Ryzen 9 5950X.

| Workload | Before median | After median | Result | Allocation volume | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| One posting at slot 99,999 in a 100,000-slot index | 773.2 ns/op | 7.2 ns/op | 107.4x faster | 0 B/op in both | 0 in both |
| 10,000-slot dense posting lookup in a 100,000-slot index | 13,205 ns/op | 10,105 ns/op | 1.31x faster | 0 B/op in both | 0 in both |
| Build 128 distinct singleton keys in a 16,384-slot index | 79,882 ns/op | 20,988 ns/op | 3.80x faster | 409,889 -> 144,184 B/op (2.84x lower) | 139 -> 7 (19.9x fewer) |

Raw samples:

- Singleton lookup: before `791.8, 763.9, 773.2, 777.8, 696.7`; after
  `7.191, 7.343, 7.432, 6.618, 7.200` ns/op.
- Dense lookup: before `12,362, 13,772, 13,205, 13,605, 12,583`; after
  `10,081, 9,934, 10,105, 10,380, 10,845` ns/op.
- Singleton build: before `81,541, 79,882, 83,812, 76,973, 73,249`; after
  `20,988, 19,555, 19,855, 21,984, 21,929` ns/op.

## Verification

- `make test-bitset-adaptive` runs the existing BitsetIndex contract tests and
  the adaptive promotion/demotion regression.
- `make race-bitset-adaptive` passes.
- `make format-bitset-adaptive` passes.
- The full package target was attempted, but the shared worktree currently
  lacks unrelated aggregate-state symbols referenced by
  `aggregate_state_registry.go`; those files were not changed here.
