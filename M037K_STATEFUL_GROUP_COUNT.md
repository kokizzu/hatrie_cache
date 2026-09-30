# M037k Stateful Differential Group Count

`hatSql.IncrementalGroupCount` is an importable, opt-in Materialize-style
differential arrangement for grouped `COUNT`. It keeps one `int64` count and
one timestamp per active group, so updates can arrive in separate batches
without rebuilding the input history.

```go
groupCount, err := hatSql.NewIncrementalGroupCount(func(row hatSql.SQLRow) string {
	return row["region"].(string)
})
if err != nil {
	panic(err)
}

changes, err := groupCount.Apply([]hatSql.DifferentialRow{
	{Key: "event-1", Time: 1, Diff: 1, Row: hatSql.Row{"region": "apac"}},
})
```

Each visible group change emits a retraction of the previous `count` row and
an insertion of the new `count` row. A group entering the result emits one
insertion; a group leaving emits one retraction. Signed duplicate weights are
preserved, negative counts and `int64` overflow are rejected, and a rejected
batch leaves the arrangement unchanged. `Snapshot` and `AllRows` return one
positive row per active group in sorted key order.

The constructor requires a group-key callback. The operator is intentionally
not wired into the default SQL planner: callers choose it when a long-lived
incremental stream is available. Existing `GroupCountDifferentialRows` remains
the batch-scoped API.

## Benchmark

Command: `make benchmark-m037k-stateful-group-count`.

The rebuild control appends one update and recomputes the complete history
after every update. The stateful streaming path applies the same 256 updates
one at a time; the stateful batch path applies all 256 at once. Five
`-benchmem` samples were measured on Linux amd64 with an AMD Ryzen 9 5950X.

| Path | Median ns/op | Median B/op | Median allocs/op | Relative CPU | Relative bytes | Relative allocations |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rebuild complete history after every update | 12,650,463 | 25,086,735 | 117,420 | 1.00x | 1.00x | 1.00x |
| Stateful one-update `Apply` | 114,254 | 185,897 | 1,223 | 110.7x faster | 134.9x lower | 96.0x fewer |
| Stateful 256-update `Apply` | 111,205 | 205,649 | 971 | 113.8x faster | 122.0x lower | 120.9x fewer |

These are maintenance-workload comparisons, not a claim that every grouped
query is 118x faster. The gain comes from retaining aggregate state instead of
reprocessing the entire history. The cost is one map entry per active group
and caller-managed lifecycle/checkpointing; planner integration and generic
stateful support for every aggregate remain future work.

Raw samples are in [`M037K_BENCHMARK_RAW.txt`](M037K_BENCHMARK_RAW.txt).
