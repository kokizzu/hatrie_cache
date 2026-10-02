# CH-059 Bitmap Equality Container Scan

This follow-up applies the same ClickHouse/Tarantool bitmap-index idea to the
single-value equality path. A bitmap equality probe now visits Roaring array
and bitset containers directly instead of first allocating a temporary
`[]uint32` from `RoaringBitmap.Values()`.

## Scope

- The index format, row cloning, locking, value normalization, and fallback
  behavior are unchanged.
- Sparse array containers and dense bitset containers use the same bounded row
  validation as the batched `IN` path.
- No new storage, configuration, or retained memory is introduced.

## Measurement

Fixture: 4,000 JSON rows, eight low-cardinality `state` values, one equality
probe, Linux/amd64, AMD Ryzen 9 5950X, `-benchtime=200ms -count=3`.

| Path | Median ns/op | B/op | Allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Before: `Values()` equality traversal | 104,192 | 179,069 | 1,025 | baseline |
| After: direct container traversal | 102,395 | 177,004 | 1,023 | 1.02x faster, 1.01x lower bytes, 2 fewer allocations |

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#ch-059-bitmap-equality-container-scan).
The gain is intentionally modest because cloning the candidate row maps remains
the dominant cost. The change removes a temporary posting-list backing only;
borrowed-row semantics are out of scope.

## Verification

```text
make verify-ch059-bitmap-equality
make race-ch059-bitmap-equality
make vet-ch059-bitmap-equality
make benchmark-ch059-bitmap-equality
```
