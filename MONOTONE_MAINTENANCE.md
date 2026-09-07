# Adaptive Monotone Maintenance

`hatSql.TypedTableAggregate.ApplyAuto` performs a conservative batch-shape
analysis before maintaining an aggregate:

```go
if hatSql.TypedTableChangesAreMonotone(changes) {
	// Every change has an After row and no Before row.
}
if err := aggregate.ApplyAuto(changes); err != nil {
	return err
}
```

An empty batch is considered monotone. A batch containing any delete/update
shape (`Before` values) or a change without `After` values is classified as
general and uses the existing `Apply` implementation. Invalid row widths,
sequence gaps, replay handling, and aggregate results therefore retain the
same authoritative validation and semantics as before. The classifier scans
the supplied batch only; it does not claim that an external source is
append-only beyond the batch being applied.

## Why It Helps

The monotone path can skip before-row removal, negative-diff bookkeeping, and
the associated checks when the batch proves that those cases cannot occur.
Callers that do not know the batch shape can use `ApplyAuto`; callers with an
already-proven append-only source can continue to call `ApplyMonotone`.
`Apply` remains available and unchanged for callers that prefer explicit
general semantics.

## Benchmark

Raw `go test -benchmem -count=5` output for 256 append-only changes on an AMD
Ryzen 9 5950X, Linux amd64. Each operation creates an aggregate and applies
the same batch, so setup cost is equal:

| Path | Sample 1 | Sample 2 | Sample 3 | Sample 4 | Sample 5 | Median | Memory | Allocs |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| General `Apply` | 19032 ns | 18571 ns | 18863 ns | 19702 ns | 21622 ns | 19032 ns | 560 B/op | 7 |
| `ApplyAuto` | 17693 ns | 17485 ns | 17252 ns | 19856 ns | 17028 ns | 17485 ns | 560 B/op | 7 |

The adaptive path was about `1.09x` faster with unchanged measured memory and
allocation count. For update/delete-heavy batches it performs a linear
classifier scan and then falls back, so use the explicit monotone method when
the caller already has a durable append-only source guarantee and needs to
avoid that scan.
