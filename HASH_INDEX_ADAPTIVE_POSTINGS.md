# Adaptive Hash Index Postings

This change applies the small-cardinality posting idea used by compact
secondary indexes in Tarantool and low-cardinality arrangements in ClickHouse
to non-unique `HashIndex` keys. A key with one ID stores that ID inline; a
second ID promotes the posting to the existing sorted slice, and deleting back
to one ID demotes it. Unique indexes are unchanged.

The public API, sorted ID order, duplicate-key behavior, and ID `0` semantics
remain unchanged. The tagged posting adds a small dense-path branch and a
slightly wider map value in exchange for eliminating one slice allocation per
singleton key.

## Measurement

Command: `make benchmark-hash-index-adaptive`

Five `-benchmem` samples on an AMD Ryzen 9 5950X:

| Workload | Before median | After median | Result | Allocation volume | Allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| One-ID exact lookup | 8.342 ns/op | 7.751 ns/op | 1.08x faster | 0 B/op in both | 0 in both |
| 10,000-ID dense lookup | 1,203 ns/op | 1,228 ns/op | 2.1% slower | 0 B/op in both | 0 in both |
| Build 128 singleton keys | 10,499 ns/op | 7,594 ns/op | 1.38x faster | 17,264 -> 17,648 B/op (2.2% higher) | 137 -> 9 (15.2x fewer) |

Raw samples:

- One-ID lookup: before `8.782, 8.342, 8.374, 8.029, 8.064`; after
  `7.553, 7.938, 7.122, 8.176, 7.751` ns/op.
- Dense lookup: before `1,108, 1,203, 1,160, 1,226, 1,220`; after
  `1,316, 1,133, 1,288, 1,228, 1,162` ns/op.
- Singleton build: before `10,436, 10,499, 10,560, 10,553, 10,369`; after
  `8,181, 7,716, 7,594, 7,543, 7,547` ns/op.

A dual-map alternative removed the dense-path regression but was rejected: its
singleton build used `21,208 B/op`, 22.8% above the baseline, and singleton
lookup was slower than the tagged map. The tagged representation keeps the
larger sparse allocation win with a bounded dense-path cost.

## Verification

- `make test-hash-index-adaptive` passes the existing HashIndex contract tests,
  the singleton promotion/demotion lifecycle, and valid ID `0` coverage.
- `make race-hash-index-adaptive` passes.
- `make format-hash-index-adaptive` passes.
- The shared root package gate still has unrelated missing aggregate-state
  symbols; the feature files do not touch that path.
