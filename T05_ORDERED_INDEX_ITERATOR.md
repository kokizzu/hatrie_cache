# T-G05 Ordered Index Iterator

## What changed

`hat/hatDataStructure` now exposes `OrderedIndex[T, K]`, a generic ordered
secondary index for read-heavy range scans. Entries are ordered by the supplied
key comparator and then by stable numeric ID.

The API is useful for `ORDER BY`, range predicates, keyset pagination, and
top-k scans that already have typed rows in memory. It is independent of the
SQL package so embedded callers can import it directly.

## API

```go
index, err := hatDataStructure.NewOrderedIndex(
    func(row Row) string { return row.Name },
    strings.Compare,
    1024,
)
if err != nil {
    return err
}

if err := index.Upsert(row.ID, row); err != nil {
    return err
}

iterator, ok := index.Seek("m")
if !ok {
    return nil
}
defer iterator.Close()
for {
    entry, next, err := iterator.Next()
    if err != nil {
        return err
    }
    if !next {
        break
    }
    consume(entry.ID, entry.Key, entry.Value)
}
```

Methods:

- `NewOrderedIndex(extractor, compare, capacity)` creates the index.
- `Upsert(id, value)` inserts or replaces an ID and reorders it if its key changes.
- `Delete(id)` removes an ID and reports whether it existed.
- `Len()` returns the number of entries.
- `Clear()` removes all entries and invalidates existing iterators.
- `First()` starts at the smallest key.
- `Seek(key)` starts at the first key greater than or equal to `key`.
- `SeekAfter(key)` starts at the first key strictly greater than `key`.
- `SnapshotInto(dst)` copies a stable sorted collection, reusing `dst` when possible.
- `OrderedIndexIterator.Next()` returns one entry without allocating.
- `OrderedIndexIterator.Close()` releases an iterator stopped before exhaustion.

## Semantics

- A mutation invalidates every existing iterator. The iterator returns
  `ErrOrderedIndexIteratorInvalidated` instead of returning a partially
  reordered view.
- An iterator must be consumed to completion or explicitly closed when a scan
  stops early. Do not copy an iterator value or use one concurrently with
  itself.
- The index itself is safe for concurrent `Upsert`, `Delete`, `Clear`, and
  iterator creation. Race coverage exercises readers and writers together.
- While an iterator is live, mutations use one immutable entry copy so the
  iterator never reads concurrently modified backing storage. With no live
  iterator, mutations use the existing in-place sorted-vector fast path.
- `SnapshotInto` remains the better choice when the caller needs a collection
  that can outlive mutations. Reuse a caller-owned destination to avoid its
  allocation.

## Benchmark

The benchmark ran on Linux, `amd64`, AMD Ryzen 9 5950X, with 10,000 integer
entries and five 200 ms samples per case. Values below show the observed
sample range, not a single best run.

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Previous per-entry read lock iterator | 64,535-65,646 | 0 | 0 |
| Current iterator | 19,151-19,413 | 0 | 0 |
| Reusable caller snapshot | 6,757-6,887 | 0 | 0 |
| Allocating caller snapshot | 34,441-48,403 | 245,760-245,762 | 1 |

The new iterator is about 3.4x faster than the previous per-entry-lock design,
and about 2x faster than an allocating snapshot. A reusable caller snapshot is
still faster for callers that can own and reuse its buffer, so the iterator is
primarily valuable for streaming and allocation-free traversal.

Mutation benchmark results use 10,000 existing entries and replace IDs in a
steady-state loop:

| Case | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| Current inactive mutation | 310,410-318,077 | 0 | 0 |
| Flat in-place baseline | 304,840-335,739 | 0 | 0 |
| Mutation with one live iterator | 205,162-212,193 | 245,760 | 1 |

Inactive writes are within benchmark noise of the flat baseline. A live
iterator intentionally adds one immutable copy per overlapping mutation; close
or exhaust iterators promptly when write throughput and memory are more
important than the scan.

## Verification

```text
make test-t-g05-ordered-index-local-clean
make race-t-g05-ordered-index
make test-t-g05-package
make vet-t-g05-ordered-index
make benchmark-t-g05-ordered-index-local-clean
make benchmark-t-g05-mutation
```

The focused, race, package, and vet targets pass. The Makefile targets are
kept as local command entry points; the implementation commit intentionally
does not include unrelated concurrent Makefile refactoring in the shared
worktree.
