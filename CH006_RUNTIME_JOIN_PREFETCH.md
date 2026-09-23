# CH-G06 Mark-Selective Join Prefetch: Rejected

This experiment evaluated an opt-in runtime join-side prefetch hook inspired by
mark-selective reads. The executor would have supplied the materialized probe
side's inclusive bounds and key count, allowing a partitioned source adapter to
fetch likely matching marked partitions first while returning the complete
logical partition set in canonical order.

The implementation was rolled back after the fair benchmark. The current
partition resolver API returns materialized rows only; it cannot overlap remote
fetches, reduce read amplification, or expose first-result latency. Adding the
request scan and scheduling work therefore made the local path slower.

## Fair measurement

Command:

```text
make m266-ch-g06-benchmark
```

Both paths used the same 256-partition, partition-backed resolver and the same
query. Five samples used `-benchtime=2s -benchmem -count=5` on an AMD Ryzen 9
5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative result |
| --- | ---: | ---: | ---: | --- |
| Existing partition resolution | 6,240,499 | 7,073,437 | 34,101 | Reference |
| Mark-selective prefetch request | 6,594,036 | 7,086,594 | 34,234 | 1.057x slower; 0.19% more bytes; 0.39% more allocations |

Raw samples:

```text
BenchmarkCH006RuntimeJoinPartitionPrefetchBaseline-32
6410316 ns/op 7073469 B/op 34101 allocs/op
6240499 ns/op 7073434 B/op 34101 allocs/op
6185413 ns/op 7073436 B/op 34101 allocs/op
6654340 ns/op 7073439 B/op 34101 allocs/op
6229097 ns/op 7073437 B/op 34101 allocs/op

BenchmarkCH006RuntimeJoinPartitionPrefetch-32
6404529 ns/op 7086587 B/op 34234 allocs/op
6594036 ns/op 7086608 B/op 34234 allocs/op
6365127 ns/op 7086590 B/op 34234 allocs/op
6945474 ns/op 7086594 B/op 34234 allocs/op
6872117 ns/op 7086594 B/op 34234 allocs/op
```

Correctness tests passed for result preservation, default-off behavior,
mixed-key fallback, and catalog-wrapper forwarding before the rollback. No
prefetch option or resolver interface was retained.

## Revisit conditions

Revisit only with a source contract that can expose asynchronous per-partition
fetch, bounded concurrency, and first-result/early-limit timing. That contract
must measure remote read amplification and tail latency directly; a method
that returns all materialized partitions cannot demonstrate this optimization.
