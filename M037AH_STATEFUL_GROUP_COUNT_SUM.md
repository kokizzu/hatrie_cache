# Stateful Differential Group COUNT + SUM

`hatSql.NewIncrementalGroupCountSumInt64` is an importable, opt-in
Materialize-style arrangement for grouped `COUNT` and `int64` `SUM`.
It keeps one `{count, sum, time}` entry per active group and applies signed
differential updates without rebuilding the complete input history.

## API

```go
grouped, err := hatSql.NewIncrementalGroupCountSumInt64(
    func(row hatSql.SQLRow) string {
        return row["region"].(string)
    },
    func(row hatSql.SQLRow) (int64, error) {
        return row["amount"].(int64), nil
    },
)
if err != nil {
    return err
}

changes, err := grouped.Apply([]hatSql.DifferentialRow{
    {
        Time: 1,
        Diff: 1,
        Row: hatSql.Row{"region": "apac", "amount": int64(10)},
    },
})
```

Each `Apply` result is a differential change stream: a changed group gets a
negative retraction for its old aggregate followed by a positive insertion for
its new aggregate. The aggregate row has `count` and `sum` fields. For the
example above, the positive change contains:

```text
Key: "apac", Diff: 1, Row: {count: 1, sum: 10}
```

Use one-row calls for low-latency streaming or pass a whole batch for atomic
validation and fewer allocations:

```go
snapshot := grouped.Snapshot()
sameRows := grouped.AllRows()
```

`Snapshot` and `AllRows` return one positive row per active group, sorted by
group key. Returned rows are detached from internal state. A group disappears
when its count reaches zero; its sum is reset to zero if it later re-enters.

## Correctness and failure behavior

- Signed `Diff` values preserve duplicate multiplicity and support retractions.
- Negative resulting counts, count overflow, sum multiplication overflow, and
  sum accumulation overflow are rejected.
- Callback errors and validation errors leave the operator unchanged,
  including for multi-row batches.
- A zero `Diff` row is ignored without calling either callback.
- Existing `GroupCountSumInt64DifferentialRows` behavior and planner defaults are
  unchanged; this operator is opt-in.

## Measurement

The benchmark uses 256 signed updates across 32 groups on Linux/amd64 with an
AMD Ryzen 9 5950X. Five samples were collected for each path. The rebuild
control recomputes every history prefix; streaming calls `Apply` once per row;
batch calls `Apply` once with all 256 rows.

| Path | Median ns/op | Median B/op | Median allocs/op | Improvement vs rebuild |
| --- | ---: | ---: | ---: | --- |
| Full-history rebuild | 13,112,752 | 24,307,275 | 117,195 | baseline |
| Stateful one-row streaming | 116,088 | 186,665 | 1,223 | 112.9x faster, 130.2x lower bytes, 95.8x fewer allocations |
| Stateful 256-row batch | 118,306 | 209,745 | 971 | 110.8x faster, 115.9x lower bytes, 120.7x fewer allocations |

Batch validation retains pending per-group state for the duration of the call,
so it uses 12.4% more transient bytes than streaming while reducing allocation
count by 20.6%. Both stateful modes avoid retaining historical input rows.
Raw samples are in [`M037AH_BENCHMARK_RAW.txt`](M037AH_BENCHMARK_RAW.txt).
