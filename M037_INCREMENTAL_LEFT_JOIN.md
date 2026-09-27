# M037: Incremental Left Outer Join

This is a Materialize-style differential operator for maintaining a keyed
left outer join under signed updates. It is implemented in `hat/hatSql` as
`IncrementalLeftJoin` and reuses the existing indexed `IncrementalJoin` state.

## Why

A rebuild-based left join scans and allocates the full result after every
change. A differential left join keeps the left and right rows indexed by
equality key and emits only the delta caused by an insert or retraction:

- a left row with no right match emits its null-extension;
- the first right multiplicity for a key retracts those null-extensions;
- the last right multiplicity re-adds them;
- matched rows continue to use the existing incremental inner-join path;
- signed multiplicities are preserved exactly.

## API

```go
join, err := NewIncrementalLeftJoin(IncrementalLeftJoinDefinition{
    LeftKey: func(row Row) (string, error) {
        return row["group"].(string), nil
    },
    RightKey: func(row Row) (string, error) {
        return row["group"].(string), nil
    },
    Merge: func(left, right Row) (Row, error) {
        return Row{"left": left["id"], "right": right["id"]}, nil
    },
    Unmatched: func(left Row) (Row, error) {
        return Row{"left": left["id"], "right": nil}, nil
    },
})
```

Updates identify the input side and carry a stable input key:

```go
updates, err := join.Apply([]IncrementalJoinUpdate{{
    Side: IncrementalJoinLeft,
    Row: DifferentialRow{
        Key: "left-1", Time: 1, Diff: 1,
        Row: Row{"id": "l1", "group": "a"},
    },
}})
```

When no right row exists, the result contains one positive unmatched row. A
right insert for group `a` returns the matched row and a negative unmatched
row. Retracting that right row reverses both deltas. `Snapshot()` returns the
current materialized result, including null-extended rows.

Rows passed to `Merge` and `Unmatched` are private operator-owned clones and
must be treated as read-only. Callback errors are reported before state is
committed; failed callbacks therefore leave the join unchanged.

## Correctness coverage

`m037_incremental_left_join_test.go` covers:

- initial unmatched left rows;
- first-right and last-right match-boundary transitions;
- signed multiplicity products;
- invalid retractions and atomic failure behavior;
- snapshot equivalence after reversals;
- a benchmark against a full rebuild.

`m037_incremental_left_join_atomic_test.go` covers the callback-error
rollback guarantee for the single-update fast path.

## Benchmark

Command:

```text
make benchmark-m037-incremental-left-join
```

Workload: 10,000 left rows, followed by one right-side update for an indexed
key. Five benchmark samples were run on the same machine.

| Path | Median ns/op | Bytes/op | Allocs/op | Relative time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Incremental left join | 1,124 | 784 | 7 | 1.0x | 1.0x |
| Rebuild left outer join | 3,202,522 | 6,883,863 | 40,002 | 2,848x slower | 8,775x higher |

Raw samples:

```text
BenchmarkM037IncrementalLeftJoin-32     903914    1124 ns/op       784 B/op       7 allocs/op
BenchmarkM037IncrementalLeftJoin-32     916053    1128 ns/op       784 B/op       7 allocs/op
BenchmarkM037IncrementalLeftJoin-32     975219    1149 ns/op       784 B/op       7 allocs/op
BenchmarkM037IncrementalLeftJoin-32     980980    1103 ns/op       784 B/op       7 allocs/op
BenchmarkM037IncrementalLeftJoin-32    1094059    1091 ns/op       784 B/op       7 allocs/op
BenchmarkM037RebuildLeftOuterJoin-32       338 3441356 ns/op   6883862 B/op   40002 allocs/op
BenchmarkM037RebuildLeftOuterJoin-32       346 3200818 ns/op   6883862 B/op   40002 allocs/op
BenchmarkM037RebuildLeftOuterJoin-32       355 3180476 ns/op   6883863 B/op   40002 allocs/op
BenchmarkM037RebuildLeftOuterJoin-32       373 3202522 ns/op   6883863 B/op   40002 allocs/op
BenchmarkM037RebuildLeftOuterJoin-32       368 3394850 ns/op   6883866 B/op   40002 allocs/op
```

The first implementation cloned the full join state for every update and was
about 3.8x slower than rebuilding on this workload. It was replaced with the
single-update fast path before delivery. That result is intentionally retained
here so future changes do not regress to the rejected design.

## Cost boundary

The common one-update path validates callbacks and then delegates one update
to the existing indexed inner join, so it does not clone the full state. A
multi-update batch still clones the indexed join before applying updates. That
preserves atomicity for callbacks and validation errors, but costs O(active
state) transient memory and work for the batch. This is the deliberate
correctness/performance boundary for this first slice.
