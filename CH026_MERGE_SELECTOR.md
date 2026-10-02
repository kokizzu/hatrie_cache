# CH-026 Merge Selector Policies

`hatStorage.CompactionMergeSelector` is an opt-in policy helper for choosing a
bounded group of immutable compaction candidates. It supports two policies:

- `CompactionMergePolicySizeTiered` groups similarly sized parts.
- `CompactionMergePolicyTimeAware` chooses the oldest candidates within an
  optional time span.

The selector is deterministic. It validates candidate names, rejects duplicate
names after trimming whitespace, enforces a maximum part count and byte budget,
and returns a copy of the selected candidate metadata. It performs no I/O,
deletes no parts, and does not schedule work. The caller owns those actions.

## Example

```go
selector, err := hatStorage.NewCompactionMergeSelector(
    hatStorage.CompactionMergeSelectorOptions{
        Policy:        hatStorage.CompactionMergePolicySizeTiered,
        MaxParts:      4,
        MaxTotalBytes: 64 << 20,
        SizeRatio:     1.5,
    },
)
if err != nil {
    return err
}

selection, err := selector.Select(candidates)
if err != nil {
    return err
}
if len(selection.Candidates) >= 2 {
    // The caller still verifies ownership and schedules the merge.
    return mergeParts(selection.Candidates)
}
return nil
```

Zero-valued options use size-tiered selection, at most four parts, a 64 MiB
total, and a 1.5 size ratio. `MaxParts` must be at least two. A zero
`MaxTimeSpan` means that time-aware selection has no time-span limit; a
non-zero value must be positive. An empty or incompatible candidate set returns
an empty selection without an error.

## Selection rules

Size-tiered selection sorts by size and name, examines bounded windows, and
keeps the largest eligible group. Every selected part must fit the configured
total-byte limit, and the largest part cannot exceed `SizeRatio` times the
smallest part. Ties are deterministic.

Time-aware selection sorts by creation time and name, examines bounded windows,
and keeps the oldest eligible group. `MaxTimeSpan`, when configured, bounds the
difference between the newest and oldest selected timestamps. Candidates must
provide a non-zero creation time for this policy.

## Measured cost

The benchmark used 256 deterministic candidates and five samples on Linux/amd64
with an AMD Ryzen 9 5950X. The legacy baseline copied and sorted candidates but
did not validate policy constraints or return a selected batch.

| Path | Median ns/op | B/op | Allocs/op | Relative cost |
| --- | ---: | ---: | ---: | --- |
| Legacy bounded sort | 20,388 | 13,688 | 4 | Baseline |
| Size-tiered selector | 31,355 | 27,488 | 8 | 1.54x CPU, 2.01x heap, 2.00x allocations |
| Time-aware selector | 23,146 | 27,488 | 8 | 1.14x CPU, 2.01x heap, 2.00x allocations |

Raw samples:

```text
Legacy:  21634, 20903, 20388, 20284, 20347 ns/op; 13688 B/op; 4 allocs/op
Size:    30578, 30780, 31355, 31632, 31870 ns/op; 27488 B/op; 8 allocs/op
Time:    24378, 23031, 23146, 23420, 23064 ns/op; 27488 B/op; 8 allocs/op
```

This cost is intentional and isolated: no default read path invokes the
selector, and existing callers can continue using direct bounded sorting. Use
the selector when deterministic size or age policy is more valuable than the
extra planning allocations. The focused tests and benchmark are wired through
`make test-round20-ch026-merge-selector` and
`make benchmark-round20-ch026-merge-selector`.
