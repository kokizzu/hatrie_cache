# MZ-028 Adaptive Arrangement Compaction

## Scope

Typed-table aggregates already cached ordered group references when dictionary
encoding was enabled. Legacy aggregates still rebuilt a full group slice and
sorted it for every `Rows()` call. The MZ-028 slice makes both modes use the
same cached ordered-reference path.

Updating an existing group keeps the order cache because its group key does not
change. When one group is added after a read, its reference is inserted into
the existing sorted order with binary search and one slice shift. Multiple
pending additions are sorted as a small delta and linearly merged into the
existing references. Deletions and partial merges retain the full-sort
fallback because they invalidate existing references. Stable repeated reads
reuse the references.

No background worker, new configuration, or default scheduling policy was
introduced. A memory/update-rate feedback policy remains future work.

## Benchmark

Commands:

```text
make benchmark-mz028-adaptive-arrangement
make benchmark-mz028-batched-merge
```

Five runs were collected before and after the change using
`BenchmarkTypedTableAggregateDictionaryEncoding/legacy/rows_existing_state`.
The table reports the median run.

| Fixture | Before ns/op | After ns/op | Improvement | Before B/op | After B/op | Before allocs/op | After allocs/op | Retained group-key estimate |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 2,048 input rows, 512 x 4 groups | 1,436,878 | 855,635 | 1.68x faster | 1,263,934 | 1,263,620 | 10,244 | 10,241 | 375,952 -> 294,472 bytes |
| 10,000 input rows, 64 x 8 groups | 328,293 | 245,158 | 1.34x faster | 320,570 | 320,259 | 2,565 | 2,562 | 93,024 -> 73,136 bytes |

The incremental-addition benchmark uses 2,048 existing groups, reads the
ordered result once, then adds one group and reads again. Median of five runs:

| Fixture | Before ns/op | After ns/op | Improvement | Before B/op | After B/op | Before allocs/op | After allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 2,048 existing groups, one new group per read | 1,577,791 | 883,679 | 1.79x faster | 1,449,067 | 1,319,110 | 12,787 | 8,798 |

The batched-delta benchmark uses 4,096 existing groups, then applies 128 new
groups and reads the ordered result. Five runs were collected with
`BenchmarkMZ028BatchedNewGroups`:

| Fixture | Before ns/op | After ns/op | Improvement | Before B/op | After B/op | Before allocs/op | After allocs/op | Tradeoff |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 4,096 existing groups, 128 new groups | 3,257,711 | 2,433,796 | 1.34x faster | 2,903,142 | 2,899,811 | 25,613 | 25,614 | 0.11% lower B/op; one additional allocation |

Raw batched samples (`ns/op`, `B/op`, `allocs/op`):

```text
before: 3339949 3272701 3257711 3156323 3211068; 2903246 2903156 2903141 2903142 2903140; 25613 25613 25613 25613 25613
after:  2426723 2432099 2433796 2440319 2434429; 2899820 2899821 2899808 2899809 2899811; 25614 25614 25614 25614 25614
```

The retained-key estimate drops because the legacy path no longer keeps a
separate encoded sort-key string per group. Per-operation output-row
allocation remains the dominant cost, so total `B/op` changes only slightly.

## Correctness

Focused coverage verifies deterministic output, cached reads, existing-group
updates without unnecessary rebuilds, incremental single-group insertion,
batched-delta merging, and the full-sort fallback after deletion:

```text
make test-mz028-adaptive-arrangement
make race-mz028-adaptive-arrangement
```

The aggregate-wide target also ran. It still reports the pre-existing typed
table arrangement checkpoint failures in
`TestTypedTableAggregateArrangementCheckpointRestoresGlobalAggregate` and
`TestTypedTableAggregateArrangementCheckpointRestoresWithoutReplay`; those
failures are unrelated to this read-path change.
