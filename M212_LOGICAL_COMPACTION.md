# M212: Logical Compaction

`hatSql.SQLRetainedState.Compact(frontier)` removes obsolete exact historical
versions while keeping the current live version readable. It advances the
retention boundary without rebuilding the current source maps or row images.

## Semantics

- A nil state returns `ErrSQLRetainedStateNil`.
- A zero frontier is a no-op.
- Versions at or below the requested frontier are discarded, except that the
  latest version is always retained so the live state remains readable.
- The current source maps and rows are not rewritten. The retained-history
  slice is rebuilt so discarded version maps and their row backing storage can
  become collectible.
- Exact `AS OF` reads for discarded frontiers return
  `ErrSQLRetainedStateFrontierUnavailable`.
- Compaction is in-memory retention management. It is not a durable journal
  checkpoint and does not replace snapshots or replication acknowledgements.

Example:

```go
state, err := hatSql.NewSQLRetainedState(hatSql.SQLRetainedStateOptions{
    MaxFrontiers: 128,
})
if err != nil {
    return err
}
// Publish frontiers 100, 101, and 102.
if dropped, err := state.Compact(101); err != nil {
    return err
} else if dropped != 2 {
    return fmt.Errorf("dropped %d frontiers", dropped)
}
// Frontier 102 remains the live and exact readable version.
```

Call compaction only after the application no longer needs exact reads at the
discarded frontiers. The method takes the same internal state lock as publish
and exact lookup, so concurrent state access remains synchronized.

## Measurement

The focused benchmark runs an empty publish followed by compaction of one old
version. It isolates the update/retention path rather than an end-to-end SQL
query. Results are medians from five samples on Linux/amd64 with an AMD Ryzen 9
5950X and `-benchmem`:

| Workload | Median ns/op | B/op | Allocs/op |
| --- | ---: | ---: | ---: |
| Empty publish plus one-version compaction | 186.8 | 120 | 3 |

Raw samples:

```text
186.2 ns/op  120 B/op  3 allocs/op
186.8 ns/op  120 B/op  3 allocs/op
188.2 ns/op  120 B/op  3 allocs/op
185.9 ns/op  120 B/op  3 allocs/op
196.1 ns/op  120 B/op  3 allocs/op
```

The benchmark measures the CPU and allocation cost of maintaining a compacted
retention window; memory reclamation depends on the discarded versions becoming
unreachable and on the Go garbage collector. Focused tests verify that current
rows remain unchanged, discarded frontiers fail cleanly, a target beyond the
latest version does not discard the latest version, and frontier zero is a
no-op.

Focused verification:

```text
make m212-format
make m212-test
make m212-race
make m212-vet
make m212-benchmark
```
