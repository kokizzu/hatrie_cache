# CH-44: Interval join maintenance

Status: implemented and benchmarked.

`IncrementalIntervalJoin` already supported differential interval joins, but its
per-join-key bucket used a sorted slice. Adding or removing an interval shifted
the slice and rebuilt the prefix maximum end value. That made high-churn
maintenance linear in the number of intervals in the bucket, even though the
overlap lookup could prune using the prefix maximum.

The bucket now uses a deterministic interval treap. Each retained entry carries
its own priority, child pointers, and subtree maximum end value. This avoids a
second heap object per interval while preserving the existing entry map and
public API. The overlap walk prunes subtrees whose maximum end cannot reach the
query interval, and in-order traversal keeps snapshot output deterministic.

## Complexity

| Operation | Previous bucket | Current bucket |
| --- | --- | --- |
| Add an interval | O(n) slice insertion and prefix rebuild | Expected O(log n) |
| Remove an interval | O(n) slice deletion and prefix rebuild | Expected O(log n) |
| Find overlaps | Prefix-pruned scan | Subtree-max-pruned scan, O(log n + matches) expected |
| Snapshot ordering | Sort active entries by key | Same deterministic key sort |

The treap priority is derived from the interval start and key, so the shape is
repeatable for the same input. The join remains intentionally non-concurrent;
callers must retain the synchronization behavior they used before this change.

## Test-first verification

Before changing the bucket implementation, the focused interval-join tests were
run through `make test-ch044-interval-join` and passed. The implementation was
then changed to the treap and the same tests were rerun. The package tests,
focused race test, and vet target are also part of the final verification.

## Benchmark

The benchmark creates 2,048 intervals in one join-key bucket and applies them in
one batch. It uses five samples with `-benchtime=250ms` and `-benchmem` on the
same AMD Ryzen 9 5950X Linux/amd64 host.

| Implementation | Median ns/op | Median B/op | Median allocs/op | Change |
| --- | ---: | ---: | ---: | --- |
| Sorted slice with prefix rebuild | 9,262,177 | 19,162,273 | 10,303 | 1.00x |
| Embedded-metadata interval treap | 2,076,175 | 1,452,249 | 8,240 | 4.46x faster, 13.19x lower B/op, 1.25x fewer allocations |

Raw samples are also recorded in [BENCHMARK.md](BENCHMARK.md#ch-44-interval-join-maintenance).
`B/op` is Go's cumulative allocation metric for the benchmark; it is not a
retained-heap measurement. The current layout adds four machine-word tree
metadata fields to each retained entry, but avoids a separate treap-node
allocation and substantially reduces transient allocation in this workload.

The benchmark covers bucket maintenance, not network transfer, storage, or
full-result materialization. The existing interval semantics, differential
weights, replacement behavior, validation, and snapshot ordering remain covered
by the interval-join tests.
