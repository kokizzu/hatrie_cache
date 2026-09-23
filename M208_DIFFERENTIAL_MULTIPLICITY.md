# M208 Differential Multiplicity Folding

`hatSql.ConsolidateQuerySubscriptionDeltas` combines query-subscription
deltas that contain the same complete row. It is useful for CDC and update
streams that represent a logical change as repeated inserts and deletes rather
than one already-collapsed `+1`/`-1` pair.

## Semantics

- Signed `int64` multiplicities are added with overflow detection.
- Zero-multiplicity rows are removed from the result.
- The first surviving occurrence determines output order.
- The input slice and its row values are not mutated.
- Surviving rows are cloned in the returned slice.
- `ErrQuerySubscriptionDeltaOverflow` is returned if a combined multiplicity
  cannot fit in `int64`.

The complete row is the equality key. A row with the same unique-key value but
different non-key columns is not merged with another row.

## Debezium integration

`hatSql.DebeziumChangefeed` keeps its existing fast path for normal batches.
If that path rejects duplicate keys or unsupported multiplicity, it retries once
after folding equal complete rows. This permits repeated equal deltas to cancel
or produce one net change without weakening the duplicate-key check for
different row images. Consolidation errors are returned before feed state is
changed.

The behavior is unchanged for the common case:

```text
old row, -1
new row, +1
```

An update stream that contains two copies of the old row and one copy of the
new row produces one update with net old multiplicity `-1` and new
multiplicity `+1`. Equal rows whose net multiplicity is zero disappear.

## Verification

Focused tests cover equal-row folding, cancellation, row ownership, overflow,
Debezium update conversion, and atomic error handling. Run:

```text
make m208-test
make m208-race
make m208-vet
```

The full package target still reports the pre-existing typed-table arrangement
checkpoint failures in `m_u05_arrangement_recovery*.go`; those are unrelated to
M208.

## Benchmark

The benchmark uses 512 distinct rows and four deltas per row (`+1`, `+1`, `-1`,
`-1`). It compares the previous manual map-and-filter implementation with the
new public consolidator. The benchmark is intentionally a consolidation-path
measurement; the normal Debezium fast path is unchanged.

Median of five samples:

| Path | ns/op | B/op | allocs/op | Relative |
| --- | ---: | ---: | ---: | ---: |
| Manual baseline | 1,247,712 | 257,250 | 10,249 | 1.00x |
| M208 consolidator | 1,238,949 | 290,082 | 10,250 | 0.993x, within noise |

Raw samples:

```text
Manual ns/op:       1274742 1257862 1247712 1216819 1225745
Manual B/op:        257254  257247  257250  257243  257259
Manual allocs/op:   10249   10249   10249   10249   10249
M208 ns/op:         1179055 1238949 1207564 1295063 1301916
M208 B/op:          290076  290082  290082  290085  290082
M208 allocs/op:     10250   10250   10250   10250   10250
```

This run's CPU median is 0.7% lower for the general-purpose API, which is
within benchmark noise; it costs about 12.8% bytes and one allocation on this
synthetic consolidation workload. The cost is limited to callers that need
multiplicity folding; ordinary Debezium batches do not enter this path.
