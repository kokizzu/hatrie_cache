# TR-053 Unique HashIndex Small Vector

This is a Tarantool-style compact representation for small unique secondary
indexes. A unique `HashIndex` keeps records in a bounded linear slice while it
has at most 32 entries. It promotes to the existing ID and key hash maps at
entry 33 and demotes back to the slice after deletion reaches 32 entries.

The change is internal. `HashIndex` lookup, duplicate rejection, update, and
delete semantics are unchanged. Non-unique indexes continue to use their
existing posting-list maps. The configured capacity is retained as a hint when
the unique index promotes, so large indexes do not pay repeated map growth.

## Measured Result

Runs used Go benchmarks with `-benchmem -count=5` on the same AMD Ryzen 9
5950X host. The baseline is the pre-change hash-map implementation.

| Workload | Before median | After median | Improvement | Before memory | After memory | Before allocs | After allocs |
|---|---:|---:|---:|---:|---:|---:|---:|
| 16-entry unique upsert | 31.05 ns/op | 14.72 ns/op | 2.11x faster | 0 B/op | 0 B/op | 0 | 0 |
| 512-entry unique upsert | 32.92 ns/op | 32.87 ns/op | 1.00x, within noise | 0 B/op | 0 B/op | 0 | 0 |
| 16-entry unique build | 1,455 ns/op | 510.7 ns/op | 2.85x faster | 1,968 B/op | 608 B/op | 9 | 2 |

The small-build representation uses 3.24x fewer allocated bytes and 4.5x fewer
allocations. The large-index control has no measured regression in the recorded
sample; the map capacity hint is preserved on promotion.

Raw samples are recorded in [BENCHMARK.md](BENCHMARK.md#tr-053-unique-hashindex-small-vector).
