# MZ-032 Late-Data Reclock

Status: adopted as an importable `hatPipeline` primitive.

This follows Materialize's reclocking model: source progress and processing
progress use different gauges, so a compact remap sidecar records which source
frontier is complete at each published processing frontier. See the
[Materialize reclocking design](https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20210714_reclocking.md).

## Why

Event-time records can arrive out of order. Applying their original event time
directly to a live result can rewrite a processing-time point that was already
published. `LateDataReclock` separates the two gauges and gives callers a
deterministic place to assign late corrections.

Frontiers are exclusive upper bounds:

- A source event at `eventTime` is covered when `eventTime < SourceFrontier`.
- A remap binding says that covered source data is available at its
  `ProcessingFrontier`.
- Source progress may be observed before it is published to the processing
  timeline.
- A late event is assigned at its arrival processing time instead of rewriting
  its earlier mapped frontier.

The component stores no row payloads. Keep payloads in the existing journal or
arrangement and persist the remap snapshot alongside that data when recovery
requires it.

## Example

```go
reclock, err := hatPipeline.NewLateDataReclock(hatPipeline.LateDataReclockOptions{
    MaxBindings: 4096,
})
if err != nil {
    return err
}

_, _ = reclock.ObserveSourceFrontier(100)
_, _, _ = reclock.AdvanceProcessingFrontier(10)

_, _ = reclock.ObserveSourceFrontier(140)
_, _, _ = reclock.AdvanceProcessingFrontier(20)

assignment, err := reclock.Assign(120, 25)
if err != nil {
    return err
}
// assignment.ProcessingFrontier == 25
// assignment.CoveredBySourceFrontier == 140
// assignment.Late == true
```

An event at source time `120` was covered by the binding at processing frontier
`20`. Since it arrived at processing time `25`, the returned assignment is a
late correction at `25`. If it arrives at processing time `15`, the assignment
is held at `20` and `HeldUntilProcessing` is true.

## API

- `ObserveSourceFrontier` advances source progress monotonically without
  publishing it.
- `AdvanceProcessingFrontier` publishes the current source progress and
  coalesces repeated source frontiers.
- `SourceFrontierAt` finds the latest source frontier at or before a processing
  point.
- `ProcessingFrontierAt` finds the earliest processing point that covers a
  source event.
- `Assign` classifies an event as on-time, held, or late without copying its
  payload.
- `CompactBefore` drops old remap points while retaining an anchor. Lookups
  before the compacted boundary return `ErrLateDataReclockHistoryCompacted`.
- `MarshalSnapshot` and `UnmarshalLateDataReclockSnapshot` provide a bounded,
  versioned binary snapshot with CRC32 validation.

The default `MaxBindings` is `4096`. Reaching the limit returns an error rather
than silently dropping history; compact explicitly before accepting another
distinct source frontier. Existing defaults and SQL execution are unchanged.

## Benchmark

Command:

```text
make benchmark-mz032-late-data-reclock
```

Linux amd64, AMD Ryzen 9 5950X, five benchmark samples, 4,096 bindings:

| Lookup | Baseline | Reclock | Improvement | Reclock bytes/op | Reclock allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: |
| Source frontier, linear scan vs binary search | 600.8 ns/op | 44.41 ns/op | 13.53x faster | 0 | 0 |
| Processing frontier, linear scan vs binary search | 1069 ns/op | 38.74 ns/op | 27.59x faster | 0 | 0 |

Raw samples:

```text
BenchmarkMZ032LinearSourceLookup: 604.3 604.8 570.8 551.0 600.8 ns/op
BenchmarkMZ032BinarySourceLookup: 43.12 40.77 44.64 44.52 44.41 ns/op
BenchmarkMZ032LinearProcessingLookup: 974.4 1042 1145 1069 1134 ns/op
BenchmarkMZ032BinaryProcessingLookup: 37.73 42.70 38.74 41.98 38.60 ns/op
```

The baseline is the same sorted binding slice with a linear scan. The
candidate uses binary search plus a read lock, so the result includes the
concurrency guard needed by the public API. Memory is bounded by the binding
limit: a binding is two `uint64` values, or 64 KiB for the default 4,096
entries before slice overhead. Snapshot and compaction intentionally allocate
only on explicit control-plane calls.

## Limits

This is a reusable remap/control-plane component, not automatic SQL planner or
connector wiring. It does not infer source frontiers, buffer payloads, or
provide a distributed timestamp oracle. Callers must publish source progress
only when their source guarantees it, persist snapshots together with source
data when needed, and choose a compaction boundary that satisfies their read
retention policy.
