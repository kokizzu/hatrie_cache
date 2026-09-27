# M037: Incremental Full Outer Join

This is the symmetric extension of the incremental left join: a keyed full
outer join maintained under signed differential updates. It uses one shared
`IncrementalJoin` index and emits only the changed matched rows and
null-extensions.

## Behavior

For each equality key:

- left rows and right rows with positive multiplicity produce matched pairs;
- a left row with no right multiplicity produces a left null-extension;
- a right row with no left multiplicity produces a right null-extension;
- the first row on one side retracts the other side's null-extensions;
- the last row on one side re-adds the other side's null-extensions;
- signed multiplicity products and retractions remain exact.

The implementation stores one row and count per stable input key, plus the
existing per-join-key buckets. It does not maintain a second independent join
or rescan the full relation after each update.

## API

```go
join, err := NewIncrementalFullJoin(IncrementalFullJoinDefinition{
    LeftKey: func(row Row) (string, error) {
        return row["group"].(string), nil
    },
    RightKey: func(row Row) (string, error) {
        return row["group"].(string), nil
    },
    Merge: func(left, right Row) (Row, error) {
        return Row{"left": left["id"], "right": right["id"]}, nil
    },
    LeftUnmatched: func(left Row) (Row, error) {
        return Row{"left": left["id"], "right": nil}, nil
    },
    RightUnmatched: func(right Row) (Row, error) {
        return Row{"left": nil, "right": right["id"]}, nil
    },
})
```

Apply changes with the existing side-tagged differential update type:

```go
rows, err := join.Apply([]IncrementalJoinUpdate{{
    Side: IncrementalJoinLeft,
    Row: DifferentialRow{
        Key: "left-1", Time: 1, Diff: 1,
        Row: Row{"id": "l1", "group": "a"},
    },
}})
```

Unmatched output keys use `0x01 + input key` for the left side and
`0x02 + input key` for the right side. Matched output keys retain the inner
join's `left-key + NUL + right-key` form. `Snapshot()` and `AllRows()` return
the complete current relation, including both null-extension types.

Rows passed to callbacks are private clones and must be treated as read-only.
Callback errors are evaluated before the single-update state is committed.
Multi-update batches run against a cloned indexed state and publish only after
all updates succeed, preserving atomicity.

## Verification

The tests cover:

- left-only and right-only rows;
- first-match and last-match transitions on both sides;
- matched retractions;
- weighted multiplicities and exact snapshots;
- callback-error rollback without state mutation.

Commands:

```text
make test-m037-incremental-full-join
make test-m037-full-join-package
make race-m037-incremental-full-join
make vet-m037-incremental-full-join
```

## Benchmark

Command:

```text
make benchmark-m037-incremental-full-join
```

Workload: 10,000 indexed left rows, alternating one right-side insert and one
right-side retraction for a key with 1,000 left rows. The rebuild path scans
and constructs the full full-outer result on every iteration.

| Path | Median ns/op | Median bytes/op | Allocs/op | Relative time | Relative bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Incremental full join | 1,837,552 | 2,071,197 | 12,029 | 1.0x | 1.0x |
| Rebuild full outer join | 3,135,394 | 5,685,727 | 30,145 | 1.71x slower | 2.75x higher |

Raw samples:

```text
BenchmarkM037RebuildFullOuterJoin-32     374 3224460 ns/op 5685727 B/op 30145 allocs/op
BenchmarkM037RebuildFullOuterJoin-32     379 3103978 ns/op 5685723 B/op 30145 allocs/op
BenchmarkM037RebuildFullOuterJoin-32     390 3135394 ns/op 5685727 B/op 30145 allocs/op
BenchmarkM037RebuildFullOuterJoin-32     369 3130608 ns/op 5685727 B/op 30145 allocs/op
BenchmarkM037RebuildFullOuterJoin-32     378 3166537 ns/op 5685725 B/op 30145 allocs/op
BenchmarkM037IncrementalFullJoin-32     661 1860359 ns/op 2071221 B/op 12029 allocs/op
BenchmarkM037IncrementalFullJoin-32     639 1857540 ns/op 2071197 B/op 12029 allocs/op
BenchmarkM037IncrementalFullJoin-32     638 1821000 ns/op 2071196 B/op 12029 allocs/op
BenchmarkM037IncrementalFullJoin-32     655 1837552 ns/op 2071196 B/op 12029 allocs/op
BenchmarkM037IncrementalFullJoin-32     663 1817563 ns/op 2071198 B/op 12029 allocs/op
```

The improvement is smaller than the left-join benchmark because a boundary
change must emit 1,000 null-extension retractions or insertions for the chosen
key. It still avoids reconstructing the other 9,000 rows and cuts measured
allocation volume by about 2.75x.

## Cost boundary

The single-update path performs constant-time index lookups plus the rows in
the affected join-key bucket. A key with many opposite-side rows necessarily
emits many differential transitions. Multi-update batches retain O(active
state) clone cost for atomic validation, matching the existing incremental
join contract.
