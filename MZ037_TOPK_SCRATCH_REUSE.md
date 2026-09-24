# MZ037 Incremental Top-K Selection Scratch Reuse

`hatSql.IncrementalTopK` already maintains an exact weighted Top-K view with a
treap. Each mutation used to allocate two temporary selection slices before
diffing the old and new bounded views. This follow-up reuses two internal
selection buffers for the single-writer `Apply` path.

The buffers are never returned to callers. Transition rows and rank changes
still clone their row payloads, so a result from one `Apply` remains valid
after a later mutation. Retention is bounded: each buffer keeps at most 4,096
selection entries. Larger `K` values use temporary buffers and do not retain
their full selection in the maintainer.

The change is opt-in in the same way as `IncrementalTopK`; SQL planner wiring,
distributed exchange, and persistence are unchanged.

## Verification

Test-first command:

```text
make test-c213-topk
```

The focused suite covers signed updates, atomic failures, randomized
correctness, returned-result ownership, and the scratch retention bound.

Benchmark command:

```text
make benchmark-c213-topk
```

Workload: 10,000 active rows, `K=20`, and repeated delete-plus-insert row
replacement. The rebuild control sorts all 10,000 rows on every update. The
incremental path seeds the relation outside the timer. CPU: AMD Ryzen 9 5950X
16-Core Processor, Linux amd64.

| Path | Before median | After median | Improvement |
| --- | ---: | ---: | ---: |
| Incremental Top-K CPU | 2,575 ns/op | 2,033 ns/op | 1.27x faster |
| Incremental Top-K transient heap | 1,222 B/op | 579 B/op | 2.11x lower |
| Incremental Top-K allocations | 7 allocs/op | 5 allocs/op | 1.40x fewer |

Raw repeated samples:

```text
Before: 2712, 2470, 2575, 2591, 2507 ns/op; 1221, 1223, 1223, 1221, 1222 B/op; 7 allocs/op
After:  2052, 2098, 1819, 1954, 2033 ns/op; 579, 581, 579, 579, 579 B/op; 5 allocs/op
```

The tradeoff is bounded retained scratch memory. Two full 4,096-entry buffers
retain approximately 128 KiB of selection metadata on a 64-bit process; the
retention test verifies that larger selections do not exceed that bound.
