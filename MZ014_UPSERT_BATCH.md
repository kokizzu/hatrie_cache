# MZ-014 Upsert Batch Consolidation

`hatDataStructure.UpsertBatch[T]` is an importable source-batch primitive for
CDC and upsert feeds. It keeps one final operation per non-empty string key:
the last value wins, while a delete remains as a tombstone that the
destination can apply.

## Usage

```go
batch := hatDataStructure.NewUpsertBatch[Order](1024)
for _, event := range sourceBatch {
	if event.Deleted {
		if err := batch.Delete(event.Key); err != nil {
			return err
		}
		continue
	}
	if err := batch.Upsert(event.Key, event.Value); err != nil {
		return err
	}
}

batch.ForEach(func(record hatDataStructure.UpsertRecord[Order]) {
	if record.Deleted {
		destination.Delete(record.Key)
		return
	}
	destination.Upsert(record.Key, record.Value)
})
batch.Reset()
```

The first-seen key order is preserved. For example, `Upsert("a", 1)`,
`Upsert("b", 2)`, `Delete("a")`, and `Upsert("a", 3)` emits `a=3`, then
`b=2`; a delete without a later upsert emits one tombstone. A delete clears
the previous value in the batch so discarded values are no longer retained.

`UpsertBatch` supports the zero value. `NewUpsertBatch(capacity)` can reserve
space for the expected distinct-key count. `Reset` clears keys and values but
retains map and slice capacity for reuse by the next source batch. The type is
not safe for concurrent use. Values are stored as provided; callers must not
mutate a mutable value while it is in the batch.

Empty keys are rejected. Keys are not trimmed or normalized because source
identity must remain exact. The structure does not interpret source offsets,
perform I/O, or automatically change SQL DML; a connector decides when to
flush the final operations. Use
[MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md](MZ013_SOURCE_CONNECTOR_CHECKPOINTS.md)
to persist the source offset after the consolidated batch has been applied.

## Why This Is Different From DifferentialMultiset

`DifferentialMultiset` consolidates equal `(data, time, diff)` entries for
differential arrangements. `UpsertBatch` instead models keyed source state:
it retains the latest value and keeps deletes even when no prior value exists
in the batch. The two structures serve different source representations.

## Benchmark And Tradeoff

The benchmark uses 10,000 source events, 1,000 distinct keys, final-output
consumption, and five samples on an AMD Ryzen 9 5950X, `linux/amd64`.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative to matching map path |
| --- | ---: | ---: | ---: | --- |
| Fresh map | 243,252 | 114,768 | 5 | `1.00x` |
| Fresh `UpsertBatch` | 183,224 | 87,424 | 7 | `1.33x` faster, 0.76x bytes, +2 allocations |
| Reused map | 220,668 | 0 | 0 | `1.00x` |
| Reused `UpsertBatch` | 163,796 | 0 | 0 | `1.35x` faster, same bytes and allocations |

The steady-state path is the intended use for repeated source batches. Fresh
construction still reduces allocated bytes and CPU but uses two more
allocations in this workload. Raw samples and the exact command are in
[BENCHMARK.md](BENCHMARK.md#mz-014-upsert-batch-consolidation).

```sh
make test-mz014-upsert
make benchmark-mz014-upsert
```
