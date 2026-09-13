# Reverse Ordered Index Iterators

Hatrie now exposes descending traversal for `hatDataStructure.OrderedIndex`.
This adopts the useful part of Tarantool's reverse index iterators and
ClickHouse's read-in-order direction: callers can consume the largest keys
first without copying the complete ordered entry slice.

## API

- `OrderedIndex.Last()` returns a live iterator at the greatest key.
- `OrderedIndex.SeekBefore(key)` starts at the greatest key strictly below
  `key`.
- `OrderedIndex.SeekBeforeOrEqual(key)` starts at the greatest key less than or
  equal to `key`.
- `OrderedIndex.LastSnapshotCursor()` returns a stable reverse cursor. Its
  `Next` method walks the snapshot from greatest to smallest key.
- `OrderedIndexSnapshotCursor.SeekBefore` and `SeekBeforeOrEqual` reposition a
  stable cursor for descending traversal.

Reverse iterators preserve the existing ordering rule: keys are ascending and
ties are ordered by ID in the forward view, so reverse traversal visits equal
keys by descending ID. Live iterators are invalidated by mutation. Snapshot
cursors retain their original view through the existing copy-on-write path and
must be closed when stopped before end-of-stream.

The feature is additive and does not change the default forward API, storage
format, wire format, or SQL planner. SQL's existing columnar and native Top-N
paths remain responsible for SQL query planning; this primitive is available
to callers that maintain an ordered index directly.

## Benchmark

Linux/amd64, AMD Ryzen 9 5950X, five 200 ms samples, 1,024 indexed entries,
`-benchmem`:

| Workload | Median | Heap | Allocations | Relative result |
| --- | ---: | ---: | ---: | --- |
| Reused materialized slice, then reverse walk | 862.2 ns/op | 0 B/op | 0 | fastest when caller already owns reusable scratch |
| Allocating materialized slice, then reverse walk | 3,647 ns/op | 24,576 B/op | 1 | baseline |
| Zero-copy reverse iterator | 2,255 ns/op | 0 B/op | 0 | 1.62x faster and 24,576 fewer bytes than allocating baseline |

The iterator is intentionally not described as faster than a caller that has
already reserved and reused a materialized slice. Its win is bounded memory
and immediate streaming, especially when the caller would otherwise allocate
or retain the full result. Reproduce with `make benchmark-tr29`.
