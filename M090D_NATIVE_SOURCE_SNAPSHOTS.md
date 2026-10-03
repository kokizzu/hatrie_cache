# M090d Native Dataflow Source Snapshots

M090d applies a Materialize-style consistency rule to the automatic native
SQL dataflow path: a source identity is read at most once during one query
execution. If the same `CACHE` source is used by both sides of a join, both
aliases consume the same source slice and therefore observe one snapshot.

## Behavior

The native executor keeps a bounded, per-query map keyed by source kind and
key. It is not a process-wide cache and is discarded with the execution
control, so writes in later queries cannot observe stale rows. The source
resolver still receives the query context on the first read.

Only the ordinary materialized-source fallback in native dataflow uses this
shared slice. Projected sources, columnar/streaming/indexed/partitioned
resolvers, and the general SQL executor keep their existing contracts. Native
operators treat source rows as read-only and build new result rows.

This also fixes the existing repeated-source regression: a resolver that
changes its result between calls must still produce identical left and right
join rows when both references have the same source identity.

## Tradeoff

The previous native join path fetched the same source twice. The new path
retains one source slice for the duration of the query and avoids the
defensive clones used by the general executor. That lowers resolver work,
heap, and allocation count. The retained source data remains bounded by the
query's existing row limit; there is no cross-query retention or invalidation
cost.

## Verification

The focused test verifies one resolver call and equal values on both join
aliases. Race and vet checks cover both `hatSql` and `hatCache`.

Reproduce with:

```text
make test-m090d-native-source-snapshots
make race-m090d-native-source-snapshots
make vet-m090d-native-source-snapshots
make benchmark-m090d-native-source-snapshots
```

The before/after benchmark uses 1,024 source rows and an equality join of the
same source with five `-benchmem` samples on Linux/amd64, AMD Ryzen 9 5950X:

| Path | Median ns/op | Median B/op | Median allocs/op | Relative time |
| --- | ---: | ---: | ---: | ---: |
| Before, two resolver reads | 1,277,281 | 1,944,084 | 12,317 | `1.00x` |
| After, one shared snapshot | 1,048,320 | 1,590,973 | 10,273 | `1.22x` faster |

The optimized path is 1.22x lower in heap bytes and 1.20x lower in
allocations. The raw samples and command output are recorded in
[BENCHMARK.md](BENCHMARK.md#m090d-native-dataflow-source-snapshots).
