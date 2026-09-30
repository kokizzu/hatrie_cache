# M065: Differential `ROW_NUMBER` and `LAG`

This is an importable, opt-in differential window primitive. It does not change
the existing append-only `IncrementalRowNumberLagWindow` or the default SQL
planner path.

## API

```go
window, err := hatSql.NewDifferentialRowNumberLagWindow(
    hatSql.DifferentialRowNumberLagWindowOptions{
        PartitionKey: func(row hatSql.SQLRow) string { return row["region"].(string) },
        Lag:          1,
        MaxRows:      100_000,
    },
)

corrections, err := window.Apply([]hatSql.DifferentialRow{
    {Key: "order-1", Time: 10, Diff: 1, Row: hatSql.Row{"region": "sg", "value": 7}},
})
```

`Apply` accepts signed `int64` weights. A positive update inserts or increases
the multiplicity of a key/time record. A negative update retracts that same
record and fails atomically if it is missing or underflows. Rows are ordered by
`Time`, then `Key`, then the repeated-copy ordinal. `SnapshotWithError` returns
the current positive state; `Snapshot` is the convenience form when callers do
not need an internal-state error.

Each update returns transition rows: negative rows retract obsolete
`ROW_NUMBER`/`LAG` results, followed by positive rows adding the new results.
The output is deterministic by partition and ordered row identity. Input rows
are cloned deeply enough to prevent later caller mutation of byte slices,
nested rows, maps, and slices from changing retained state.

`MaxRows == 0` uses the bounded default of 1,000,000 logical rows. Set a lower
limit for untrusted or memory-constrained workloads. A nil partition function
uses one partition.

## Implementation Choices

- `Lag <= 1` tail appends, tail weight changes, and tail retractions use a
  constant-work metadata path and append/truncate the partition order slice.
- A single non-tail update reconstructs only the affected partition from its
  already ordered rows; it does not clone the global state or sort a map.
- Partition membership is maintained as a sorted slice of key/time IDs. This
  removes the per-partition membership-map iteration and sort from snapshots
  while keeping the global key lookup map for update validation.
- Multi-update batches retain the correctness-first copy-on-write path. They
  remain atomic and only clone affected partition membership slices.

The primitive is deliberately not wired into all SQL window execution. Callers
that can provide stable keys and signed changes can use it directly; automatic
planner integration remains a separate design decision.

## Measurements

The benchmark uses a one-partition, 1,024-row workload, `LAG(1)`, one insert
and one matching retraction per operation, five `100ms` samples, Linux amd64,
AMD Ryzen 9 5950X. The rebuild control materializes both old and new windows
and emits the same correction shape.

| Workload | Differential median | Full rebuild median | Relative result |
| --- | ---: | ---: | --- |
| Tail update CPU | 76,898 ns/op | 4,848,617 ns/op | 63.05x faster |
| Tail allocation bytes | 1,946 B/op | 3,828,871 B/op | 1,967.6x lower |
| Tail allocations | 15/op | 41,186/op | 2,745.7x fewer |
| Front update CPU | 2,681,924 ns/op | 2,450,608 ns/op | 0.91x; 9.4% slower |
| Front allocation bytes | 5,403,791 B/op | 5,333,127 B/op | 1.01x; 1.3% higher |
| Front allocations | 16,456/op | 16,553/op | 1.01x; 0.6% fewer |

The front case is inherently correction-heavy: almost every existing row
changes its row number or lag value. The differential operator keeps the
correctness and bounded-state guarantees, but the simple slice control remains
slightly faster for that worst case. The fast tail case is the intended high
throughput path. Full raw samples are checked in as
[`M065_BENCHMARK_RAW.txt`](M065_BENCHMARK_RAW.txt); rerun with
`make benchmark-m065`.

## Verification

The feature workflow is:

```text
make format-m065
make test-m065
make race-m065
make vet-m065
make benchmark-m065
```

The focused and race tests cover weighted inserts/retractions, out-of-order
updates, partitioning, deep row cloning, deterministic corrections, bounds,
row identity validation, and failure atomicity. A full package run currently
also exposes unrelated pre-existing RowBinary decimal/enum/IP test failures;
those are outside this feature's touched files.
